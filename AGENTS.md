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
- Kafka produces: gateway.webhooks.raw
- Kafka consumes: none
- Storage: PostgreSQL webhook_ingress

Contracts are frozen in `cbl-protos` (CF-1 for gRPC, CF-2/CF-3 for events), see `CBL-004`.

## 3. Architecture

Layers: `internal/transport` -> `internal/usecase` -> `internal/domain` -> `internal/storage`.
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
(event write budget, default `450ms`, allowed `100ms` - `5s`).

## 6. Development rules

- Pipeline: SDD spec in `.specs/` -> architecture -> contracts -> tests first -> code ->
  review by 4 agents (no-context, architecture, with-context, security - blocking) ->
  AGENTS.md -> final runs -> linters -> commit.
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
| CBL-16.11 | 2026-10-16 | done (English comments and documentation across the repository: code, tests, configs, all four specs, AGENTS.md, README; e2e runs via `test/e2e/E2E.md` with scenario E2E_WebhookIngress_Kafka; validation run PASS; coverage unchanged) | `.specs/CBL-16.11-conventions.md` | this commit |

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

1. `403 ip_forbidden` responses: the peer address is outside the provider `ip_allowlist`
   (log `webhook source rejected` with `remote_addr`); on a legitimate gateway address
   change update the allowlist; behind an LB remember that the peer address is the LB
   address (KI-webhook-ingress-04).
2. `429 rate_limited` responses (log `webhook rate limited`): a delivery burst exceeded
   `rate_rps`/`rate_burst`; check whether the provider duplicates events (duplicates are
   safe but consume tokens), and under sustained legitimate traffic raise the limit.
3. Empty `ip_allowlist` for a provider: dev mode, any addresses are accepted; in prod
   fill it in (KI-webhook-ingress-04).
