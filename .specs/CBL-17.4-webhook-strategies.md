# CBL-17.4. Webhook Ingress: per-provider verification strategies and response policies

| Field | Value |
| :-- | :-- |
| Epic | E17 (task CBL-17.4) |
| Repository | `cbl-webhook-ingress` |
| Size | M |
| Wave and parallel group | W5-PROVIDER, stage B |
| Depends on | CF-4 (`cbl-protos@cf-4`: `signature_state`, `fetch_required`), `cbl-kit` 17.3 (`webhooksig`) |
| Contract version | consumes `cbl-protos@cf-4`/`cf-4.1`, `cbl-kit` 7900510 |
| Status | done |

## 1. Context and goal

Provider webhooks differ: BTCPay and BitPay sign the raw body (HMAC-SHA256 hex or base64),
NOWPayments signs the canonical JSON (HMAC-SHA512), CoinGate echoes a shared token and
forbids 4xx answers for registered callbacks (CBL-003 section 4). The task replaces the
single legacy HMAC scheme with a per-provider strategy registry on top of the kit
`webhooksig` package, fills the CF-4 verification metadata (`signature_state`,
`fetch_required`, the deprecated `signature_verified`), implements the per-provider
response policies, provider dedup keys, token redaction and the dynamic CoinGate
allowlist. The legacy scheme (t/v1 header over `<unix>.<body>`) stays for existing
providers (`mock`), so the e2e smoke and `cbl-mock-gateway` keep working until 17.5.

## 2. Requirements

FR-1. Config (additive JSON of `WEBHOOK_PROVIDERS`, legacy compatible):

- `verification`: `{"strategy": "raw_hmac|canonical_json_hmac|token_echo", "header": ...,
  "algorithm": "sha256|sha512" (default sha256), "encoding": "hex|base64" (default hex),
  "prefix": ..., "field": ... (token_echo, default token)}`; when absent, the legacy
  fields (`signature_header`, `event_id_header`) select the `legacy_hmac` strategy.
- `fetch_required` (bool): the delivery needs a provider fetch before a canonical event
  (CoinGate, BitPay).
- `event_key`: `{"header": ...}` or `{"template": "{data.id}:{data.status}"}` (mutually
  exclusive): the vendor event key for the deterministic `event_id`; the template
  resolves dotted paths over the JSON body (scalars only, `json.Number` literals kept).
  Absent means the legacy `event_id_header`.
- `ip_allowlist_url`: dynamic allowlist source (CoinGate ips-v4 list); merged with the
  static `ip_allowlist`; the enabled/disabled state follows the union of both.
- Validation: strategy-specific required fields, unknown strategies, mutually exclusive
  event key sources, malformed URLs are `ErrInvalidConfig`; a provider with a `verification`
  block but without any event key source (`event_key` or `event_id_header`) is rejected at
  startup; `token_echo` requires `fetch_required` (its status model always needs the fetch);
  secrets stay via `secret_env`.

FR-2. Verification (domain, uses `webhooksig`):

- `Verifier` interface returns `VerificationState` (`verified`, `hmac_failed`,
  `echo_mismatch`, `absent`); mapping: nil -> verified, `ErrSignatureAbsent` -> absent,
  `ErrEchoMismatch` -> echo_mismatch, `ErrSignatureMismatch`/`ErrMalformed` -> hmac_failed;
  the legacy verifier maps a missing header to absent and all rejections to hmac_failed.
- Verification order is unchanged: allowlist, endpoint, intake limit, body limit,
  signature (CBL-5.3).

FR-3. Response policies (CBL-003 section 4 hardening):

- `raw_hmac`, `canonical_json_hmac`, `legacy_hmac`: verified -> 200 and record; any
  non-verified -> 401 and NOT recorded (the provider retries; no storage flooding).
- `token_echo` (CoinGate): never 4xx/3xx for the hook route (a 4xx or redirect disables
  callbacks), including an unresolvable vendor key, which falls back to a random `event_id`
  with a warning: verified -> 200 and record; echo_mismatch/absent -> 200, the raw delivery
  is recorded with the non-verified state for observability (random `event_id`, the
  deterministic dedup key of a genuine delivery is not poisoned) and a warning is logged;
  aggregating alerts are 17.18.

FR-4. Event metadata (CF-4):

- `GatewayWebhookRaw`: `signature_state` is always filled; `signature_verified` (deprecated)
  mirrors `state == VERIFIED` during the transition; `fetch_required` comes from the
  provider config (CoinGate and BitPay set it true regardless: the connector fetch is
  mandatory for their status models).
- `event_id` envelope header: verified deliveries keep the deterministic
  UUIDv5(provider + vendor event key) (CBL-5.1 FR-4a); unverified recorded deliveries use
  a random UUIDv7.

FR-5. Dedup keys: `event_key.header` (BTCPay deliveryId) and `event_key.template`
(NOWPayments `{payment_id}:{payment_status}`, CoinGate `{refund_id}:{refund_status}` or
`{id}:{status}` as `|`-separated alternatives, BitPay `{data.id}:{data.status}`); an
unresolvable or empty key is 422 `missing_event_id` for HMAC strategies (token echo keeps
200 and uses a random id, see FR-3). The resolved key and the body-extracted gateway id are
bounded to 128 bytes: an unbounded provider value in the Kafka key or envelope headers
would stall the ordered outbox relay (poison pill).

FR-6. Dynamic allowlist: a refresher fetches each `ip_allowlist_url` (HTTPS only, redirects
are not followed) on a schedule (`WEBHOOK_ALLOWLIST_REFRESH`, default `10m`, 0 disables;
a configured URL with refreshes disabled is a startup error), parses newline/comma-separated
IPs and CIDRs (catch-all and unspecified entries are rejected: a poisoned source must not
widen itself to everything), atomically replaces the provider set; on a fetch, size or parse
error the last known set is kept and a warning is logged; the first load runs before the
intake starts, and a URL-configured provider stays fail-closed until the first successful
load; a provider with neither a static list nor a URL stays the dev fail-open mode with the
existing warning.

FR-7. Token redaction: for `token_echo` the `field` value of the body is replaced with an
empty string before the raw event is recorded or published (the verification secret never
lands in the outbox, Kafka or logs); when the body cannot be rewritten safely (malformed,
non-object, trailing data) the raw body is not stored at all and an error is logged; headers
keep the existing sensitive filter minus the signature header of the provider.

FR-8. Quality: unit tests for the strategy matrix, policies, event keys, redaction,
allowlist parsing/merge/refresh; existing unit, integration and e2e scenarios stay green
(`mock` uses the legacy scheme unchanged); coverage at least 80%; lint, vet, em dash
check green.

Out of scope: connector-side fetch-to-verify and fail-closed publication (17.2), mock
gateway provider profiles (17.5), aggregated mismatch alerting (17.18), retention of raw
bodies (14.2).

## 3. Work plan

Spec -> tests -> domain (verification, event key, allowlist) -> transport/usecase/main
wiring -> battery (unit, integration, lint, emdash) -> commit
`CBL-17.4: provider webhook strategies and policies` -> umbrella CBL-003 sync (unverified
publication wording, §6 exception for the deterministic `event_id`).

Review closure (4 reviewers, 16.10.2026): findings closed by the follow-up commit:
bounded vendor keys and gateway ids (the outbox poison pill), token echo never answers 4xx
(unresolvable key -> random event id, `|` template alternatives for payment and refund
shapes), strict token redaction with a body-not-stored fallback, HTTPS-only refresh URLs
without redirects, catch-all and unspecified entries rejected in dynamic lists, fail-closed
intake before the first dynamic load (with a synchronous first refresh and a startup error
on disabled refreshes), `token_echo` requiring `fetch_required`, English problem messages,
spec/AGENTS/runbook sync.
