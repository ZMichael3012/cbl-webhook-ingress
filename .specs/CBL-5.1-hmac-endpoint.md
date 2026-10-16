# CBL-5.1. Webhook endpoint with HMAC verification

| Field | Value |
| :-- | :-- |
| Epic | E5, Webhook Ingress |
| Repository | `cbl-webhook-ingress` |
| Size | M |
| Wave and parallel group | W1-WEBHOOK |
| Depends on | 1.6 (cbl-kit/hmac), CF-1 |
| Contract version | `cbl-protos@cf-1` (`billing/events/v1`), `CBL-003` section 4 |
| Status | done (locally, awaiting commit) |

## 1. Context and goal

The service accepts provider webhooks, verifies the signature over the raw body, persists the
event in the inbox and publishes it to `gateway.webhooks.raw` via the outbox, responding
quickly (target up to 500 ms). This is the first line of defense of the money circuit against
forged events.

## 2. Boundaries

In scope: the route `POST /hooks/{provider}/{endpoint_id}`, raw body reading, HMAC verification,
inbox by event id, outbox publication, limit protection.
Out of scope: event normalization (3.5 Connector), topics and schemas (CF-1), IP allowlist (5.3).

## 3. Functional requirements

FR-1. `POST /hooks/{provider}/{endpoint_id}`: the body is read as raw bytes, the limit is 1 MB
(`http.MaxBytesReader`), the signature is verified via `cbl-kit/hmac` before any JSON parsing.

FR-2. Signature headers are configured per provider: the signature header name, the event id
header name, and an optional endpoint_id allowlist (`WEBHOOK_PROVIDERS` JSON in env:
`{provider: {signature_header, event_id_header, secret_env, endpoints?}}`). An empty `endpoints`
allows any endpoint_id; a non-empty one allows only the listed ones. Secrets come only from
env/Vault and never appear in logs.

FR-3. Success: in a single PostgreSQL transaction, an inbox record `(consumer=provider, event_id)`
and an outbox row with the `gateway.webhooks.raw` envelope (`GatewayWebhookRaw` fields: provider,
gateway_id, received_at, headers without sensitive headers, raw_body, signature_verified=true);
the response `200 {"status": "accepted", "duplicate": <bool>}` within ~500 ms without waiting for
Kafka (the duplicate field shows a recognized duplicate).

FR-4. Signature verification is performed before event header validation (a single `401` for
unauthenticated requests). A duplicate `event_id` (inbox conflict) is `200` without
re-publication. A body over the limit is `413`. An unknown provider or an unregistered
endpoint_id is `404`. Invalid JSON after signature verification does not affect acceptance: the
body is stored as is. If `gateway_id` is not found in the body, the partition key is built from
endpoint_id, and the envelope `gateway_id` field gets the value `unknown` (clarification in 3.5).

FR-4a. An event without an id in the headers is rejected with `422` (idempotency is impossible).
For the inbox, `event_id` is deterministically derived as UUIDv5 (`uuid.NewSHA1`) from
`"cbl-webhook:" + provider + ":" + vendor_event_id`: a repeated delivery by the provider is
recognized without an external mapping table, while the inbox UUID requirement is satisfied.

FR-5. Publication to Kafka is performed by the relay from `cbl-kit/outbox` (a background
goroutine); outbox lag metrics are connected in 0.7.

FR-6. Local dependencies: `replace` directives to `../cbl-kit` and `../cbl-protos` (see
`CBL-2.1`, FR-7).

## 4. Contracts

The topic `gateway.webhooks.raw`, the key `provider+gateway_id`, the envelope and headers are in
`CBL-003` section 6; proto `billing/events/v1/GatewayWebhookRaw`.

## 5. Data model and migrations

`migrations/00001_outbox.sql` and `00002_inbox.sql` are copies of the reference DDL from
`cbl-kit`.

## 6. State machines

Not applicable; payment statuses are changed by Payment based on canonical events.

## 7. Edge cases

Empty body, invalid hex, timestamp outside the +/- 5 minute window (replay), multiple signatures
(secret rotation), unknown endpoint_id, duplicate event, concurrent duplicates (unique inbox
key), slow PostgreSQL (the response degrades, but 200 is not guaranteed).

## 8. Non-functional requirements

Response p99 under 500 ms under burst (5.4); the service does not parse JSON before signature
verification; the raw body is not logged (only its size and event_id); secrets never reach logs
or traces.

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Smoke | `go build ./...`, `/healthz` |
| Unit | httptest: valid signature, invalid signature, expired timestamp, missing id, large body, unknown provider (DB-less paths via interfaces) |
| Integration | testcontainers PostgreSQL: inbox+outbox in a single transaction; a duplicate does not re-publish; the relay sends the event (recording publisher) |
| E2E | signed webhook -> PostgreSQL (inbox/outbox) -> Kafka; implemented on testcontainers (`test/e2e`), the deviation from the literal compose is recorded in CBL-004 |
| Load | profile 5.4: p99 under 500 ms |

## 10. Work plan and commit

Commit `CBL-5.1: webhook endpoint with HMAC and inbox`; the spec is in the same commit.

## 11. Open questions

- The provider list and their signature formats are fixed together with the gateway choice
  (KI-01); until then, the mock provider configuration is used.

## 12. Review (CBL-001, step 6)

Four independent passes (13 - 14.10.2026); no blocker found, majors closed.

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | no 404 for an unknown `endpoint_id`; the UUIDv5 formula differed from the spec; the `duplicate` field was not described; e2e was missing | the `endpoints` allowlist in the provider config + test; FR-4a aligned with the code; FR-3 extended with the `duplicate` field; e2e (signed webhook -> PostgreSQL -> Kafka) added and executed |
| Architecture | `gateway_id=unknown` is a hidden assumption; no `traceparent` in the envelope | the assumption is recorded (fallback to endpoint), traceparent is planned for 0.7 (KI) |
| With context | missing relay test; empty e2e directory | relay test added; e2e implemented |
| Security | the `event_id` check before the signature produced distinguishable responses; all headers were stored in the event | the checks were reordered (signature first), sensitive headers are filtered out, read/write timeouts added, vulnerable dependencies updated |
