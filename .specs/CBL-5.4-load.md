# CBL-5.4. Webhook burst load profile

| Field | Value |
| :-- | :-- |
| Epic | E5, Webhook Ingress |
| Repository | `cbl-webhook-ingress` |
| Size | S |
| Wave and parallel group | W1-WEBHOOK |
| Depends on | 5.2, 5.3 |
| Contract version | `cbl-protos@cf-1`, `CBL-003` section 4 |
| Status | done, review closed |

## 1. Context and goal

The `CBL-002` startup threshold (section 8): webhook burst, response p99 under 500 ms, no
5xx. The profile confirms the 5.2 response budget, the 5.3 limits and the ability of the
service to accept a provider delivery burst without server errors.

## 2. Boundaries

In scope: a k6 burst profile against the service compose stand; a PostgreSQL degradation
profile (verification of `503` with `Retry-After`); recording of the results and closure
of KI-webhook-ingress-02.

Out of scope: the global mixed profile of 3000 RPS (umbrella repository, 14.1),
PostgreSQL tuning, Kafka load.

## 3. Functional requirements

FR-1. Burst profile (`PROFILE=burst`, default): `ramping-arrival-rate`
50 -> 500 rps (one ramp-up stage of 10 s), a plateau of 500 rps for 30 s, a decline to
50 rps over 5 s; each request is a unique signed event (HMAC over the raw body, a unique
`X-Event-Id` per run), the expected response is `200` with `status=accepted` and
`duplicate=false`. Thresholds: `p(99) < 500 ms`, `checks > 0.999`,
`http_req_failed < 0.001`, no 5xx (the `server_errors == 0` counter),
`rate_limited == 0`, `unexpected_responses == 0`, iteration drops under 1%
(`dropped_iterations < 192` out of ~19 125 requests).

FR-2. Degradation profile (`PROFILE=degraded`): PostgreSQL paused, 20 rps for 15 s; the
expected response of every request is `503 write_timeout` with `Retry-After: 1` and the
marker `write_timeout` in problem+json (the 5.2 write budget), any other responses are
recorded as `unexpected_responses` (threshold 0), iteration drops under 1
(`dropped_iterations < 3`); there is no event loss: after the pause is lifted a repeated
delivery is processed idempotently (covered by integration test 5.2).

FR-3. The intake limit on the stand (5.3) is configured above the profile
(`rate_rps=1000`, `rate_burst=2000` in compose): there must be no `429` in the burst
profile (`rate_limited == 0`).

FR-4. The launch commands are recorded in the header of `test/load/webhook_burst.js`
(cmd.exe and PowerShell); k6 is launched with the `grafana/k6:2.3.0` container.

## 4. Contracts

External contracts do not change: the profile uses
`POST /hooks/{provider}/{endpoint_id}` with the HMAC header from `CBL-003` (section 4).

## 5. Data model and migrations

Not applicable; the stand uses the existing migrations 00001 - 00002.

## 6. State machines

Not applicable.

## 7. Edge cases

`429` on limit overflow (not expected, is recorded); `503` while PostgreSQL is paused
(expected in the degradation profile); k6 client timeouts are higher than the server ones
(ReadTimeout 15 s); a repeated run with a new `RUN_ID` is not a replay (unique event
ids); an empty `KAFKA_BROKERS` (the outbox accumulates, the response does not degrade).

## 8. Non-functional requirements and results

Burst profile (locally, Docker Desktop, compose stand, 500 rps plateau, 19124 requests):

| Metric | Value | Threshold |
| :-- | :-- | :-- |
| Response p99 | 25.36 ms | under 500 ms |
| 5xx | 0 | 0 |
| Share of other errors (`http_req_failed`) | 0% | under 0.1% |
| Dropped iterations | 0 | under 1% (`count<192`) |
| `429 rate_limited` | 0 | 0 |
| `unexpected_responses` | 0 | 0 |

Degradation profile (PostgreSQL paused, 20 rps, 301 requests): 100% of responses were
`503 write_timeout` with `Retry-After: 1`, p99 459.7 ms (write budget 450 ms plus
overhead), `unexpected_responses = 0`, no iterations dropped; after the pause is lifted,
repeated deliveries are processed idempotently (integration test 5.2), and the
accumulated outbox is published by relay (the topic is created by `kafka-init`).

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Load | `PROFILE=burst` (thresholds above, the run is performed); `PROFILE=degraded` (503 + Retry-After, no other 5xx, the run is performed) |
| Smoke | `make build`, `make test`, existing unit/integration/e2e green |

## 10. Work plan and commit

`test/load/webhook_burst.js` (burst/degraded profiles, HMAC on k6/crypto), a run on the
compose stand, recording of the results in the spec and AGENTS. Commit
`CBL-5.4: webhook burst load profile`; the spec is in the same commit.

## 11. Open questions

- The degradation profile paused PostgreSQL manually (compose pause/unpause); automation
  is the global chaos tests 14.3.
- The degradation profile revealed and confirmed a classification defect (fixed by this
  commit): the deadline on establishing a connection to PostgreSQL expires on Linux as
  `os.ErrDeadlineExceeded` ("i/o timeout"), not as `context.DeadlineExceeded`.
  Evidence: a build without the fix produced 34 out of 301 unclassified responses, with
  the fix - 0 (a repeated pre-fix experiment: 34, then 0).
  An attempt to narrow the classification with a `writeCtx.Err() != nil` check turned out
  to be unsound: the socket deadline fires before the context manages to record the
  expiration (9 out of 301 in the run), so an i/o timeout in the write path is classified
  as budget exhaustion by error type. In the Handler the only external I/O is PostgreSQL;
  when other network calls appear, the classification will need to be refined (recorded
  in a code comment).
- The global mixed profile (14.1) includes a webhook share of 5%; the local threshold is
  compatible.
- The topic `gateway.webhooks.raw` is created by the `kafka-init` service in compose
  (discovered by the profile: with `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` relay could
  not publish).

## 12. Review (CBL-001, step 6)

Four independent passes (14.10.2026). No blockers. The MAJOR of the architectural review
(an unreliable explanation of the classification defect) was closed by a repeated
experiment: the pre-fix build reproduced 34 unclassified responses, the post-fix build -
0; the mechanism was clarified (a dial deadline on Linux yields `os.ErrDeadlineExceeded`;
a context state check is unsuitable because of a race - 9 out of 301), and the
classification by error type is documented. The remaining findings were closed by fixes
to the script, the spec and compose.

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | FR-1 described a different ramp-up; degraded verified only the presence of `Retry-After`; the dropped thresholds were weaker than declared; the classification was wider than the budget; a warning without `zap.Error`; kafka-init without a dependency for webhook-ingress; the PowerShell commands were incomplete | FR-1 aligned with the script; checks of `Retry-After: 1`, `write_timeout`, `status=accepted`, `duplicate=false`; the thresholds tightened; the classification documented (context state race - 9/301); `zap.Error` added; `depends_on kafka-init` added, empty line; the PowerShell line in the header |
| Architecture | MAJOR: the explanation of the defect cause was not reproducible, a pre-fix recheck was required; a warning without an error; transport knows about net/pgx (inherited); kafka-init without `service_completed_successfully`; step 7 of the process not done | experiment: pre-fix 34/301, post-fix 0/301; the mechanism clarified in section 11 and in a comment; `zap.Error`; the classification deliberately stays in transport (pattern 5.2); the dependency added; the journal/KI/AGENTS updated by this commit |
| With context | FR-1 against the script; kafka-init race; the degraded dropped threshold; step-7 updates deferred; duplication of the test handler; wording of section 12; the `Retry-After` check; 1 partition against the global catalog | all closed: the spec, `depends_on`, the thresholds, AGENTS/CBL-004, `dialTimeoutHandler` reused as blocking, section 12 filled in; headers/body are checked; 1 partition is a deliberate local stand convention (like ledger) |
| Security | only NITs: image pinning, `:ro` mount, the secret default, the breadth of the classification, the `Retry-After` value | secrets/kafka-init/compose/dependencies are clean; the classification narrowed and covered by a test; `Retry-After: 1` is checked; pinning and the secret default are a dev convention (0.2/0.7) |
