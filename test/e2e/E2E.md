# E2E cbl-webhook-ingress: webhook intake to Kafka on testcontainers

Run instruction for the local e2e scenario of the service. A run is performed by an
agent platform-independently: docker (testcontainers) and the Go toolchain. There are no
PowerShell run scripts (customer decision of 16.10.2026, `CBL-002` section 7).

Repository: `cbl-webhook-ingress`. Scenario stand: testcontainers Kafka+PostgreSQL
started by the test itself (`test/e2e/webhook_kafka_test.go`, build tag `e2e`). The
compose stand (`deploy/docker-compose.yml`) exists for the k6 load profile
(`test/load/webhook_burst.js`) and is not used by this scenario.

## 1. Scenarios

| Scenario | Checks | Contracts |
| :-- | :-- | :-- |
| `E2E_WebhookIngress_Kafka` | HMAC-signed webhook accepted with a fast response, the raw body persisted (inbox), the event envelope published to `gateway.webhooks.raw` (outbox + relay), duplicate delivery is idempotent, signature policy and header policy on real Kafka and PostgreSQL | CBL-5.1 (HMAC endpoint), CBL-5.2 (inbox/outbox budget); REST `POST /hooks/{provider}/{endpoint_id}` |

## 2. Isolation and parallelism

- Run mark: UTC lowercase `yyyymmddthhmmssz`.
- Artifact directory: `test/e2e/.runs/<mark>/` (create before the start).
- The scenario manages its own containers (testcontainers random ports); parallel runs
  of other services are allowed. Two parallel runs of this scenario are allowed as well
  (no fixed host ports); do not run the compose stand at the same time as the k6 profile
  on the same host ports (5432, 9092, 8081, 8060, 9260).
- All commands run from the repository root.

## 3. Common run order

1. `<MARK>` = current UTC time in lowercase; create `test/e2e/.runs/<MARK>/`.
2. Prepare: `go build ./...` and `go vet -tags "integration e2e" ./...` (output to
   `prepare.log`). Docker must be running (testcontainers pulls
   `confluentinc/cp-kafka:7.6.1` and `postgres:16-alpine` if not cached).
3. Execute the scenario: `go test -tags e2e -count=1 -v ./test/e2e/` with output to
   `run.log` (timeout of the target: 12 minutes, `make test-e2e` wraps the same command).
4. Extract the per-test results from `run.log` into `e2e-checks.txt` (one OK/FAIL line
   per test and subtest, with the verdict of the `ok github.com/cbl/cbl-webhook-ingress/test/e2e`
   line).
5. `summary.md` by the `.additionals/summary.template.md` template.
6. On failure: keep `run.log`, the failing assertion output and `docker ps -a` at the
   moment of the failure (testcontainers logs are echoed into `run.log`).

## 4. E2E_WebhookIngress_Kafka

The scenario is fully contained in the Go test; expected behavior from the specs:

- a valid `X-Billing-Signature` HMAC over the raw body is accepted; the response is fast
  (inside the write budget) and carries the duplicate flag semantics (T-WH-01);
- the raw webhook is persisted durably (inbox) and the event envelope appears in
  `gateway.webhooks.raw` with the deterministic UUIDv5 `event_id` (T-WH-02) and the
  header policy applied (first values only, sensitive and signature headers dropped,
  T-WH-03);
- a repeated delivery of the same vendor event is idempotent (`duplicate=true`, one
  envelope);
- invalid or missing signature is rejected.

Checks go to `e2e-checks.txt`.

## 5. Mandatory run artifacts

- `summary.md` (template `.additionals/summary.template.md`; PASS/FAIL verdict, steps,
  key values, artifact list, time);
- `prepare.log`, `run.log`, `e2e-checks.txt`;
- on failure: `docker ps -a`.

## 6. Related local runs (not part of this scenario)

- k6 load profile on the compose stand: commands and profiles (burst/degraded) are in
  the header of `test/load/webhook_burst.js`; artifacts follow the CBL-002 section 7
  conventions under `test/e2e/.runs/` or the load run directory.

## 7. Auxiliary files (.additionals)

- `summary.template.md` - the `summary.md` template;
- `inventory.md` - the list of auxiliary files and their purpose.

## 8. Cleanup

Testcontainers removes its containers automatically (t.Cleanup); if a run is interrupted,
remove leftovers with `docker ps -a` + `docker rm -f`. `.runs/` is not committed; a
repeated run must start from a clean state.
