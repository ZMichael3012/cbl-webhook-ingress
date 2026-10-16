# CBL-5.2. Inbox by event ID and outbox with a response budget

| Field | Value |
| :-- | :-- |
| Epic | E5, Webhook Ingress |
| Repository | `cbl-webhook-ingress` |
| Size | M |
| Wave and parallel group | W1-WEBHOOK |
| Depends on | 5.1, 1.2 (cbl-kit/outbox), 1.3 (cbl-kit/inbox) |
| Contract version | `cbl-protos@cf-1` (`billing/events/v1`), `CBL-003` sections 4, 6 |
| Status | done, review closed |

## 1. Context and goal

The core mechanics of 5.2 (inbox by `event_id`, the `gateway.webhooks.raw` outbox envelope, the
relay, the response without waiting for Kafka) were implemented in 5.1 beyond the task plan
(FR-3, FR-4 of spec 5.1). Task 5.2 closes the remaining acceptance criteria: idempotency under
concurrent deliveries of the same event and protection of the response budget (~500 ms) against
PostgreSQL degradation.

## 2. Boundaries

In scope: single-write guarantees under concurrent duplicates, a write deadline with a
deterministic response, capturing envelope invariants in tests.
Out of scope: normalization (3.5), body/signature (5.1), limits and allowlist (5.3), load
profile (5.4), outbox lag metrics (0.7).

## 3. Functional requirements

FR-1. Concurrent deliveries of the same event create no more than one row: exactly one inbox row
`(consumer=provider, event_id)` and exactly one outbox row per event; exactly one response among
the concurrent ones has `duplicate=false`, the rest have `duplicate=true` (all 200). This is
ensured by the unique inbox key and `ON CONFLICT DO NOTHING` in `inbox.Guard`; the transaction
that loses the race does not write to the outbox.

FR-2. Response budget: the `Handler.Handle` call is bounded by the `WEBHOOK_WRITE_TIMEOUT`
deadline (450 ms by default, allowed range 100 ms - 5 s; an empty value means the default, an
invalid one means the service does not start). When the deadline is exceeded, the transaction is
aborted (the pgx context cancels the query and rolls back the transaction), and the client
receives `503 application/problem+json` with `Retry-After: 1`. The event is not considered
accepted; the provider retries the delivery, and idempotency (FR-1) excludes duplicates. A
client-side cancellation (`context.Canceled`) is classified separately: an `info` level log, a
`408` response with no side effects (the response does not matter, the error rate metric is not
polluted). Other write errors remain `500`.

FR-3. Envelope invariants are pinned down by tests: the topic `gateway.webhooks.raw`, the event
type `gateway.webhook.raw`, the deterministic `event_id` = UUIDv5 from
`"cbl-webhook:" + provider + ":" + vendor_event_id` (`CBL-5.1` FR-4a), the schema `v1`, the
partition key `provider:gateway_id` (fallback `provider:endpoint_id`), the envelope field
`signature_verified=true`, the headers `provider`, `vendor_event_id`, `endpoint_id`.

FR-4. A duplicate and a timeout are distinguished in logs: a duplicate is `info` with provider
and vendor_event_id, a write timeout is `warn` with provider and the budget duration.

## 4. Contracts

The external HTTP contract does not change (`CBL-003`, section 4): only the `503` response with
`Retry-After` is added when the budget is exhausted. The envelope and headers are in `CBL-003`,
section 6; proto `billing/events/v1/GatewayWebhookRaw` (CF-1).

## 5. Data model and migrations

No new migrations: the inbox/outbox schema was fixed in 5.1 (`migrations/00001_outbox.sql`,
`00002_inbox.sql`).

## 6. State machines

Not applicable.

## 7. Edge cases

N simultaneous identical deliveries (FR-1 verification); repeated delivery after a 503 (the
event is processed, there are no duplicates); slow PostgreSQL (503 and provider retries);
client-side cancellation (the context is canceled, the response does not matter); a deadline
greater than the server `WriteTimeout` (clamped to 5 s); an invalid `WEBHOOK_WRITE_TIMEOUT`
(startup failure); a deadline less than 100 ms (clamped); an event with and without `gateway_id`
in the body.

## 8. Non-functional requirements

Response p99 under 500 ms under burst (measured in 5.4); the default write deadline of 450 ms
leaves headroom for network and serialization within the 500 ms budget; 503 must not occur under
normal load (verified in 5.4: no 5xx).

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Unit | transport: deadline exhausted -> 503 + `Retry-After`; other errors -> 500; domain: `WEBHOOK_WRITE_TIMEOUT` parsing (default, clamps, error) |
| Integration | PostgreSQL: 8 concurrent deliveries of the same event -> one inbox row, one outbox row, one `duplicate=false`; a real deadline with the inbox table locked -> 503 and rollback, a retry after the lock is released -> 200 `duplicate=false`; envelope invariants (event_id, type, schema, headers) |
| E2E | the existing scenario 5.1 (unchanged) |
| Load | 5.4 |

## 10. Work plan and commit

Tests first: unit tests for the deadline and configuration parsing, integration tests for
concurrent duplicates. Then the implementation: `domain.ParseWriteTimeout`, the `WithWriteTimeout`
option, main wiring. Commit `CBL-5.2: inbox and outbox with a response budget`; the spec is in
the same commit.

## 11. Open questions

- T-WH-02 (reuse of a vendor id with a changing status) remains open until the gateway choice
  (KI-01): the current event delivery contract assumes a unique id per event; with different
  provider behavior the key will include the status before 3.2 is chosen.
- The retry storm under prolonged PostgreSQL degradation (each 503 with `Retry-After: 1`
  provokes a retry) is accepted deliberately (T-WH-04); the behavior and metrics are verified by
  load test 5.4 (KI-webhook-ingress-02); if the storm is confirmed, jitter or a more
  conservative `Retry-After`.
- The outbox lag metric is task 0.7 (unchanged).
- The `cbl-kit/outbox` guide describes `event_id` as UUIDv7, while raw webhooks use a
  deterministic UUIDv5 (FR-3): the envelope accepts any UUID; the library docstring will be
  fixed in the next kit task.

## 12. Review (CBL-001, step 6)

Four independent passes (14.10.2026). No blockers; the architecture review major (the real DB
deadline was not covered by a test) was closed by an integration scenario with a table lock;
the remaining findings were closed by fixes: a separate `context.Canceled` branch, the clamp in
`WithWriteTimeout`, exclusion of the signature header from the stored headers, document
alignment (CBL-003, AGENTS, journal).

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | the section 7 edge cases (retry after a 503, slow PostgreSQL) were not covered; `event_id` UUIDv5 versus the CBL-003 wording; the journal runs ahead of the process; no clamp in the option; weak regression protection of the test; the concurrency test does not check `duplicate=true` | an integration test of the real deadline with rollback and retry was added; a note about UUIDv5 in CBL-003 section 6; section 12 filled in by this block; a clamp in the option; a test with `select`/timeout; checking `fresh == 1` among the 8 responses |
| Architecture | MAJOR: the deadline through the real gorm -> pgx chain was not tested (a narrow race with `sql.ErrTxDone`); `context.Canceled` was logged as Error; UUIDv5 versus CBL-003; T-WH-04 is not linked to KI-01 | an integration test of the lock, rollback and retry; the `context.Canceled` branch (info, 408); a note in CBL-003; the link to KI-01 is recorded in T-WH-04 and this section |
| With context | the T-WH-02 reference to 5.2 contradicts the fact; the journal/CBL-004 run ahead of the process; a potential `gin.SetMode` race under `-race`; the test budget is not set explicitly; the warn log has no request_id | the reference was corrected to 3.2/KI-01; finalization by this block; the inherited pattern is recorded in KI-webhook-ingress-01 (CI, 0.2); the budget is set explicitly in the test; request_id will be added with observability in 0.7 |
| Security | the 503 retry storm (MINOR, not a blocker); no clamp in the public option; the signature header was stored in the envelope; return code 0 on a configuration error (inherited) | the link to 5.4/KI-02 is recorded in section 11; the clamp was added; the signature header is excluded from `flattenHeaders`; the return code pattern is inherited behavior (0.2/0.7) |
