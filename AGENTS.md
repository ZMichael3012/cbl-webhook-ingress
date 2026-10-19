# AGENTS.md - cbl-webhook-ingress

Provider webhook intake: HMAC verification over the raw body, inbox, fast response.

Mandatory global rules: `../../AGENTS.md`, `../../.specs/CBL-001-development-process.md`,
`../../.specs/CBL-002-repos-and-docker.md`, contracts `../../.specs/CBL-003-contracts.md`.
The em dash (U+2014) is forbidden, only the hyphen is used.

## 1. Boundaries

In scope: POST /hooks/{provider}/{endpoint_id}, HMAC, inbox, outbox to gateway.webhooks.raw.

Out of scope: other platform services; interaction only through `cbl-protos` contracts
(gRPC), Kafka and the perimeter REST (see `CBL-000`, section 2).

## 2. Contracts

- REST: POST /hooks/{provider}/{endpoint_id}
- gRPC: none
- Kafka produces: gateway.webhooks.raw (CF-4 metadata: `signature_state`,
  `fetch_required`, the deprecated `signature_verified` mirror)
- Kafka consumes: none
- Storage: PostgreSQL webhook_ingress

Contracts are frozen in `cbl-protos` (CF-1..CF-4), see `CBL-004`.

## 3. Architecture

Layers: `internal/transport` -> `internal/usecase` -> `internal/domain` -> `internal/storage`.
Per-provider verification strategies (17.4): `legacy_hmac` (the platform t/v1 scheme,
existing providers), `raw_hmac` (BTCPay, BitPay), `canonical_json_hmac` (NOWPayments),
`token_echo` (CoinGate); the strategies come from the kit `webhooksig` package, the
response policies and the dedup event keys are per provider (domain).
Provider gateway id (17.26): the optional per-provider `gateway_id` template resolves the
provider payment identifier the connector fetch path needs (`{payment_id}` NOWPayments,
`{data.id}` BitPay; alternative templates separated by `|` follow the event_key grammar);
without the block the legacy top-level scan (`gateway_id`/`gatewayId`/`id`, strings and
exact JSON numbers) applies, so the numeric CoinGate id resolves as well; an unresolvable
template stores `unknown` and the connector fails closed at the fetch step (the NOWPayments
invoice-to-payment mapping stays open on the connector side, KI-connector-15).
Libraries: the standard set from `CBL-002` (Gin, GORM + pgx driver, franz-go + pkg/sr, go-redis,
goose, zap). `AutoMigrate` is forbidden, migrations are goose-only, forward only.

## 4. Commands

```sh
make build        # go build ./...
make test         # go test ./...
make vet          # go vet ./...
make lint         # golangci-lint in Docker
make up           # docker compose -f deploy/docker-compose.yml --profile app up -d --build
make down         # docker compose -f deploy/docker-compose.yml down -v
```

k6 load run (5.4): the commands and the burst/degraded profiles are in the header of
`test/load/webhook_burst.js` (run against the compose stand).
E2E runs follow the `test/e2e/E2E.md` instruction (scenario E2E_WebhookIngress_Kafka; artifacts in `test/e2e/.runs/<mark>/`); there are no PowerShell run scripts.

## 5. Ports and environment

HTTP `8060`, gRPC `-`, metrics `9260`.
Variables (applicable to the service): `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`, `PG_DSN`,
`KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`, `REDIS_ADDR`, `WEBHOOK_WRITE_TIMEOUT`
(event write budget, default `450ms`, allowed `100ms` - `5s`),
`WEBHOOK_ALLOWLIST_REFRESH` (dynamic provider allowlist refresh, default `10m`,
`0` disables, clamped to `30s` - `24h`).

## 6. Development rules

- Pipeline (TDD-first, CBL-001): an SDD spec with the architecture sketch and the full test
  plan (unit and integration matrices, e2e scenario descriptions) -> contracts -> red tests
  with recorded evidence -> implementation -> 4-agent review on the staged diff (no-context,
  architecture, with-context, security - blocking) -> AGENTS.md -> final runs (unit +
  integration + local e2e) -> linters -> commit.
- Unit+integration coverage is at least 80%; money scenarios only on real PostgreSQL and
  Kafka (testcontainers); local e2e in docker-compose is mandatory.
- Money only int64 in minor units plus decimals; float is forbidden.
- Godoc docstrings are mandatory in English: the first line starts with the entity name and ends with a period; comments explain why; errors are wrapped with `%w`; panics in business code are forbidden.
- Commit: `CBL-<epic>.<number>: <short message>`, the spec with the same prefix in the same commit.

## 7. SLO and metrics

SLO is specified in task 14.4. Base metrics: latency p95/p99, error rate, outbox lag,
consumer lag; dashboards after task 0.7.

## 8. Trade-offs

| ID | Decision | Pros | Cons and compensation |
| :-- | :-- | :-- | :-- |
| T-WH-01 | Duplicate response extended with the `duplicate` field | the client can see the repeat without a repeated effect | contract extension | recorded in CBL-5.1 FR-3 |
| T-WH-02 | `event_id` is a deterministic UUIDv5 derived from provider and vendor id | deduplication without a mapping table, the inbox requires a UUID | the vendor id must be unique per event | recorded in FR-4a 5.1; if ids are reused, include the status in the key - to be decided together with the gateway choice (3.2, KI-01) |
| T-WH-03 | Only the first header values are included in the event; sensitive headers and the signature header are dropped | security and predictability | multi-value headers are lost | the policy is defined in 5.1/5.2/5.3; the sensitive header list grows together with providers |
| T-WH-04 | Write budget `WEBHOOK_WRITE_TIMEOUT` (450 ms): when exceeded, the transaction is aborted, response `503` + `Retry-After: 1` | the response stays within the 500 ms budget under PostgreSQL degradation; the provider retries | provokes delivery retries under DB load | inbox idempotency excludes duplicates (5.2 FR-1); the threshold is verified by the 5.4 load test |
| T-WH-05 | Gateway allowlist by TCP peer address (CIDR), proxy headers are not read | `X-Forwarded-For` spoofing does not bypass the protection; no dependency on the deployment scheme | behind a proxy/load balancer the peer address is the proxy address, the real gateway address is not distinguishable | the allowlist is filled with LB addresses; network restrictions - 0.3 (NetworkPolicy/firewall), transport-level access restriction - 0.6 (mTLS there is inter-service, the external gateway is separate); KI-webhook-ingress-04 |
| T-WH-06 | Per-provider intake limit in-process (token bucket, no Redis) | the critical write path does not depend on Redis (T-02/T-11); zero overhead | with multiple replicas the limit is approximate (per-instance); the bucket resets on deploy; tokens are consumed before signature verification | aggregate protection is network-level (0.3: NetworkPolicy/firewall, LB limits); the 7.5 perimeter does not see webhooks; in prod the allowlist is mandatory (scanners will not exhaust the legitimate gateway bucket); revisit with multiple replicas |
| T-WH-07 | Per-provider strategies and response policies (17.4): raw HMAC mismatches are `401` and dropped; token echo (CoinGate) always answers `200` (never 4xx, an unresolvable key falls back to a random `event_id`) and records unverified deliveries with a non-verified `signature_state` and a random `event_id` | callbacks of CoinGate are never disabled by a 4xx; attack attempts stay observable; only verified deliveries use the deterministic dedup key | unverified deliveries consume storage and intake tokens; the connector must fail closed (17.2) | the intake limit and the allowlist bound the volume; the vendor key and gateway id are bounded to 128 bytes (outbox poison-pill guard); aggregated mismatch alerts - 17.18; the connector drops non-verified raws without canonical events |
| T-WH-08 | Dynamic provider allowlist (17.4): fetched over HTTPS without redirects on a schedule and merged with the static list; catch-all and unspecified entries are rejected; a fetch failure keeps the last known set; the first load runs before the intake, a URL-only provider stays fail-closed until then | CoinGate IP rotations do not need deploys; no fail-open window and no list poisoning | refreshes disabled with configured URLs is a startup error; static `0.0.0.0/0` stays an explicit operator option | refreshes are read-only GETs over TLS to the provider domain; failures are logged and visible; the static list stays as the floor |
| T-WH-09 | The `gateway_id` source is per-provider configuration (a template over the JSON body), not hardcoded per provider | providers nest the identifier differently (`payment_id`, `data.id`); the ingress core stays provider-agnostic and the operator declares the shape, the same pattern as `event_key` | a broken template silently sends every delivery to the connector with `unknown` | the template is validated at startup (empty or non-compilable templates are configuration errors); the connector rejects a fetch without a gateway id (fail closed) |
| T-WH-10 | The legacy top-level scan accepts JSON numbers and renders them via `json.Number` | the real CoinGate callback carries a numeric `id`; a float64 round-trip could lose precision on large ids | a numeric id now becomes the Kafka partition key where it previously resolved nothing | the 128-byte bound and the drop behaviour stay; the connector stores the decimal string form it created the order with |
| T-WH-11 | An unresolvable template yields an empty id (the envelope stores `unknown`) | fail closed: the connector rejects a fetch without a gateway id instead of guessing; token-echo providers still answer 200 | the delivery cannot be attributed and ends in the connector DLQ until the mapping exists | the event key stays independent (dedup is unaffected); the gateway id length bound and the partition-key fallback (endpoint id) apply |

## 9. Known issues

| ID | Problem | Impact | Plan | Status |
| :-- | :-- | :-- | :-- | :-- |
| KI-webhook-ingress-01 | CI and pre-commit are not configured | linters are run manually | task 0.2 | open |
| KI-webhook-ingress-02 | The load profile (5.4) is not written | the p99 threshold below 500 ms is not confirmed; behavior under DB degradation (503 retry storm, T-WH-04) is not measured | CLOSED 14.10.2026: `test/load/webhook_burst.js`, burst profile (p99 25.36 ms, 0 errors) and degraded profile (100% 503 + Retry-After, p99 459.7 ms); the `os.ErrDeadlineExceeded` classification defect was found and fixed | closed |
| KI-webhook-ingress-03 | traceparent is not propagated into the event envelope | end-to-end tracing of the asynchronous link is incomplete | task 0.7 | open |
| KI-webhook-ingress-04 | Behind a proxy/load balancer the allowlist sees the proxy address, not the gateway; the real client address is not verified | protection is weakened if the LB is reachable from outside | direct intake in prod; when an LB appears - allowlist by LB addresses plus network restrictions (0.3) and gateway access restriction (0.6) | open |

## 10. Task journal

| CBL | Date | Status | Spec | Commit |
| :-- | :-- | :-- | :-- | :-- |
| CBL-5.1 | 2026-10-14 | done, review closed (HMAC endpoint, allowlist, e2e with Kafka, coverage 84.5%) | `.specs/CBL-5.1-hmac-endpoint.md` | 73e1dbd |
| CBL-5.2 | 2026-10-14 | done, review closed (concurrent idempotency, 450 ms write budget with 503, envelope invariants) | `.specs/CBL-5.2-inbox-outbox.md` | f3d567a |
| CBL-5.3 | 2026-10-14 | done, review closed (peer IP allowlist, in-process intake limit, check order) | `.specs/CBL-5.3-protection.md` | 404c665 |
| CBL-5.4 | 2026-10-14 | done, review closed (k6 burst/degraded profiles, p99 30.66 ms; os.ErrDeadlineExceeded classification fix; kafka-init on the stand) | `.specs/CBL-5.4-load.md` | 299002e |
| CBL-16.11 | 2026-10-16 | done (English comments and documentation across the repository: code, tests, configs, all four specs, AGENTS.md, README; e2e runs via `test/e2e/E2E.md` with scenario E2E_WebhookIngress_Kafka; validation run PASS; coverage unchanged) | `.specs/CBL-16.11-conventions.md` | a4d6c62 |
| CBL-17.4 | 2026-10-16 | done, review closed (per-provider verification strategies: legacy_hmac for existing providers, raw_hmac/canonical_json_hmac/token_echo for BTCPay/BitPay/NOWPayments/CoinGate over kit `webhooksig`; response policies (token echo always 200, unverified recorded with random event_id, HMAC 401 and dropped); CF-4 metadata `signature_state`/`fetch_required` with the deprecated mirror; provider dedup keys (header or `{a.b}` template with `|` alternatives, bounded to 128 bytes) and strict token redaction; dynamic allowlist refresher (HTTPS, no redirects, catch-all rejection, fail-closed until the first load); review follow-up: outbox poison-pill bounds, English problem messages; unit coverage total 82.5%; integration and e2e green; lint 0 issues) | `.specs/CBL-17.4-webhook-strategies.md` | 3c0ab79, review follow-up in this commit |
| CBL-17.26 (ingress increment) | 2026-10-19 | done, review closed (provider gateway_id extraction: the per-provider `gateway_id` template resolves `{payment_id}` (NOWPayments) and `{data.id}` (BitPay); the legacy top-level scan learns exact numeric ids via `json.Number` so the real CoinGate callback resolves; an unresolvable template stores `unknown` and the connector fails closed at the fetch step; template validation at startup; unit and e2e regression green) | `.specs/CBL-17.26-ingress-gateway-id.md` | this commit |

## 11. Runbook

Filled in by the first product task of the service. Typical incidents: dependency
unavailability, error growth, processing lag; actions: check health/metrics, logs with
request_id, the state of the Kafka consumer group and outbox lag, restart the service if needed.

Webhook intake (5.2):

1. `503 write_timeout` responses: PostgreSQL does not fit into `WEBHOOK_WRITE_TIMEOUT`
   (logs `webhook write budget exceeded`); the provider will redeliver, there will be no
   duplicates. Actions: check the DB load and the connection pool; under sustained
   degradation raise the budget within 5 s.
2. `duplicate=true` in the response: a redelivery of an already accepted event; no action
   is required, the event is published once.
3. The outbox grows, events are not published: check `KAFKA_BROKERS` and the cbl-kit relay
   logs; with an empty `KAFKA_BROKERS` the relay is disabled (a warning in the log).

Intake protection (5.3):

1. `403 ip_forbidden` responses: the peer address is outside the static and dynamic
   provider allowlists (log `webhook source rejected` with `remote_addr`); on a legitimate
   gateway address change update the allowlist; behind an LB remember that the peer
   address is the LB address (KI-webhook-ingress-04).
2. `429 rate_limited` responses (log `webhook rate limited`): a delivery burst exceeded
   `rate_rps`/`rate_burst`; check whether the provider duplicates events (duplicates are
   safe but consume tokens), and under sustained legitimate traffic raise the limit.
3. Empty `ip_allowlist` for a provider: dev mode, any addresses are accepted; in prod
   fill it in (KI-webhook-ingress-04).

Verification strategies (17.4):

1. `invalid_signature` 401: an HMAC strategy rejected the delivery (log `webhook signature
   rejected` with the state); check the provider secret in Vault and the strategy
   configuration; the provider retries.
2. Log `webhook verification failed but the delivery is recorded`: a token echo mismatch
   or an absent token was recorded with a non-verified state; the connector never turns it
   into a canonical event; a burst of such records indicates a wrong secret or an attack.
3. Log `dynamic allowlist refresh failed`: the provider list URL is unreachable; the last
   known set stays active; check egress and `WEBHOOK_ALLOWLIST_REFRESH`.

## 12. Sandbox webhook testing (E17, 19.10.2026)

- Secrets: the repository-root `.env` (gitignored) carries the
  `WEBHOOK_PROVIDERS` JSON with the `nowpayments` (canonical_json_hmac),
  `coingate` (token_echo, `fetch_required`) and `bitpay` (raw_hmac base64,
  `fetch_required`) entries plus `NOWPAYMENTS_IPN_SECRET`, `COINGATE_IPN_TOKEN`
  (our own generated echo secret, sent by the connector as the order token
  after the KI-connector-14 fix) and `BITPAY_WEBHOOK_SECRET`.
- Stand: `docker compose -p cbl-wh-now -f deploy/docker-compose.yml -f <override.yml> --profile app up -d --build --wait`;
  the override file (kept outside the repository) replaces `WEBHOOK_PROVIDERS`
  when the committed stand carries the mock provider only; the intake listens
  on 8060.
- Tunnel: the cloudflared quick tunnel to `http://localhost:8060` (commands in
  the connector AGENTS.md section 12); the provider webhook URL is
  `https://<host>/hooks/<provider>/<endpoint_id>`; an empty `endpoints` list
  accepts any endpoint id; quick tunnel URLs are ephemeral.
- CoinGate note: the dynamic allowlist refresh (`/v2/ips-v4`) and every other
  call to CoinGate from this host need the local NL proxy (Cloudflare blocks
  the stand IP); the dev stand leaves `ip_allowlist` empty (any address) and
  omits the URL. In stage the refresh needs the same proxy path.
- Verification checks: NOW `x-nowpayments-sig` (canonical JSON HMAC-SHA512),
  CoinGate `token` echo, BitPay `x-signature` (base64 raw HMAC). A wrong secret
  surfaces as `invalid_signature` (HMAC strategies) or an unverified record
  (token echo, always 200).
- BitPay: the webhook entry is configured (raw_hmac base64, `fetch_required`),
  but the BitPay webhook end-to-end run is POSTPONED together with the BitPay
  acceptance (the account-side mock pay blocker, 030118; details in the
  connector AGENTS.md section 12 and the umbrella 17.0 section 6.2).
