# summary.md run report template

Used for service local e2e runs (`test/e2e/E2E.md`). Copy into
`test/e2e/.runs/<UTC mark>/summary.md` at the start of the run and fill it in as the run
progresses.

```markdown
# E2E run: cbl-webhook-ingress, <UTC run start mark>

- Stand: testcontainers Kafka+PostgreSQL (started by the scenario)
- Code version: `<commit hash>`, branch `<branch>`
- Start: <UTC RFC 3339>, End: <UTC RFC 3339>
- Verdict: PASS | FAIL

## Steps

| # | Step | Command (short) | Status | Artifact |
| :-- | :-- | :-- | :-- | :-- |
| 1 | prepare | go build / go vet -tags integration,e2e | OK | prepare.log |
| 2 | scenario | go test -tags e2e -count=1 -v ./test/e2e/ | OK | run.log |
| 3 | checks | per-test verdicts | OK | e2e-checks.txt |

## Key checks

| Scenario | Check | Expected | Actual | Status |
| :-- | :-- | :-- | :-- | :-- |

## Emergency dumps

<list on FAIL: docker ps -a, failing assertions from run.log>

## Notes

<deviations, retries, known flakes>
```
