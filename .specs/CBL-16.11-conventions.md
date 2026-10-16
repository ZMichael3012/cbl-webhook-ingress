# CBL-16.11. cbl-webhook-ingress: English comments and documentation; e2e via E2E.md

| Field | Value |
| :-- | :-- |
| Epic | E16, `Convention alignments` |
| Repository | `cbl-webhook-ingress` |
| Size | S |
| Wave and parallel group | W0-CORR |
| Depends on | `CBL-16.1` (convention), `CBL-16.2` (pilot pattern) |
| Contract version | not applicable (no contract changes) |
| Status | done, review closed |

## 1. Context and goal

Two cross-cutting customer corrections (16.10.2026): (1) comments and documentation are
written in English in all files (code, tests, specs, AGENTS.md, README, configurations;
log and error messages stay unchanged as executable contracts); (2) e2e runs are not
PowerShell scripts but an agent instruction `test/e2e/E2E.md` with run artifacts in
`test/e2e/.runs/`. The convention is `CBL-002` section 7. This repository had no
PowerShell run scripts (the e2e scenario is a Go test on testcontainers); the task
formalizes the run instruction and translates everything.

## 2. Scope

In scope: English translation of all comments, docstrings, config/test comments, all
four specs (CBL-5.1/5.2/5.3/5.4), AGENTS.md and README.md; `test/e2e/E2E.md` (scenario
`E2E_WebhookIngress_Kafka`: prepare, run, per-test checks, artifacts, the related k6
load profile noted separately); `.gitignore` (`.runs/`), Makefile comments.

Out of scope: changing behavior and runtime messages (problem+json `detail` texts, zap
log fragments quoted in specs as contracts), the k6 load profile itself
(`test/load/webhook_burst.js` stays), other repositories (16.12 - 16.16).

## 3. Requirements

FR-1. Language: no Russian comments or docstrings remain; Russian string literals remain
only as runtime problem+json details; test failure descriptions are English; godoc
docstrings keep the entity-name first line and end with a period; AGENTS.md development
rules state the English documentation rule.

FR-2. E2E: `test/e2e/E2E.md` contains the scenario description, isolation notes
(testcontainers, no fixed ports), the run order (prepare, `go test -tags e2e -count=1 -v ./test/e2e/`),
the expected behavior list from the specs, mandatory artifacts and cleanup; there are no
PowerShell run scripts; AGENTS.md references the instruction.

FR-3. Auxiliary files live in `test/e2e/.additionals/` (summary template, inventory);
`.runs/` is gitignored.

FR-4. Validation run: the scenario executed per `E2E.md`, artifacts in
`test/e2e/.runs/<mark>/`, `summary.md` filled with a PASS verdict.

## 4. Plan

Comment and documentation translation (parallel by file groups: cmd/domain/usecase/
transport, specs, AGENTS.md/README/configs/e2e/load) -> E2E.md and `.additionals` ->
validation run -> 4 reviews -> commit `CBL-16.11: English comments and docs; e2e via E2E.md`.

## 5. Reviews and runs

Verified: unit+integration tests green (no production code changes), lint 0, the e2e
scenario run per `E2E.md` with run artifacts, em-dash check clean, comment language
verified by a Cyrillic scan (remaining Cyrillic: problem+json detail literals in
`internal/transport/server.go`; none are comments).
