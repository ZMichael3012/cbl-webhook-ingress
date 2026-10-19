# CBL-17.26 ingress increment: provider gateway_id extraction

Status: in progress (19.10.2026). Task 17.26, ingress increment. Repository:
`cbl-webhook-ingress`. Related: connector increment A
(`cbl-connector` `.specs/CBL-17.26-connector-sandbox-fixes.md`) and the umbrella
17.0 section 6.2/7.

## 1. Goal

The connector fetch path resolves the platform payment through
`provider_invoices` keyed by the raw delivery `gateway_id`. The ingress extracts
that id only from the top-level legacy keys (`gateway_id`, `gatewayId`, `id`)
and only when the value is a JSON string, so:

- a NOWPayments IPN (top-level `payment_id`) arrives with `gateway_id=unknown`;
- a BitPay IPN (`data.id` nested) arrives with `gateway_id=unknown`;
- a CoinGate callback with the numeric `id` the real API sends (for example
  `1042`) also resolves nothing through the string-only scan.

All three make the connector fail closed at the fetch step (retry then DLQ) and
block the 17.26 sandbox runs. The reconciliation reconnaissance found the gap
while preparing the NOW/BitPay paths. Note: for NOWPayments the resolved
`payment_id` still does not match the invoice id stored in the connector
mapping until KI-connector-15 is closed; the CoinGate and BitPay paths gain the
identifier they need.

## 2. Scope

In scope:

1. `ProviderConfig` gains an optional `gateway_id` template (dotted path over
   the JSON body, the `event_key` template grammar and bounds).
2. The legacy top-level scan stays the fallback and learns numeric ids: a JSON
   number renders exactly (`json.Number`, no float rounding).
3. The transport resolves the id through `config.ExtractGatewayID(body)`; the
   local extractor is removed. The length bound and the drop behaviour stay.
4. `.env.example` gains the NOWPayments (`{payment_id}`) and BitPay
   (`{data.id}`) examples.

Out of scope: the connector contract (the raw delivery carries the resolved
gateway id) and the verification strategies.

## 3. Decisions

| ID | Decision | Rationale |
| :-- | :-- | :-- |
| T-WH-09 | The `gateway_id` source is per-provider configuration (a template), not hardcoded per provider | providers nest the identifier differently (`payment_id`, `data.id`); the ingress core stays provider-agnostic, the operator declares the shape (same pattern as `event_key`) |
| T-WH-10 | The legacy scan accepts JSON numbers and renders them via `json.Number` | the real CoinGate callback carries a numeric `id`; a float64 round-trip could lose precision on large ids |
| T-WH-11 | An unresolvable template yields an empty id (the envelope stores `unknown`) | fail closed: the connector rejects a fetch without a gateway id instead of guessing; token-echo providers still answer 200 |

## 4. Test plan

| ID | Case | Expected |
| :-- | :-- | :-- |
| G-01 | `gateway_id` template `{payment_id}` and `{data.id}` over matching bodies | the resolved id |
| G-02 | template over a body without the path | empty (unknown) |
| G-03 | legacy keys without a template: string `gateway_id`, camelCase `gatewayId`, numeric `id`, nested `data.id` | strings and numbers resolve; nested does not (top-level only) |
| G-04 | config validation: empty template, unclosed placeholder, constant template | configuration error |
| G-05 | alternative templates `{missing}\|{payment_id}` | the second alternative resolves (the event_key grammar) |
| T-01 | transport: a NOW-like delivery, a BitPay-like nested delivery, a numeric CoinGate id and an oversized resolved id | the resolved value reaches `handler.lastInput.GatewayID`; an oversized id is dropped |

E2E: the change is exercised by the 17.26 sandbox runs (increment B) and by the
existing E2E_WebhookIngress_Kafka regression.

## 5. Red evidence

Run 19.10.2026, `go test ./internal/domain/ ./internal/transport/`:

```
internal\domain\gateway_id_test.go:15:52: unknown field GatewayID in struct literal of type domain.ProviderConfig
internal\domain\gateway_id_test.go:15:71: undefined: domain.GatewayIDConfig
internal\domain\gateway_id_test.go:16:30: now.ExtractGatewayID undefined (type domain.ProviderConfig has no field or method ExtractGatewayID)
FAIL	github.com/cbl/cbl-webhook-ingress/internal/domain [build failed]
--- FAIL: TestHandleWebhookGatewayIDTemplate (0.00s)
--- FAIL: TestHandleWebhookGatewayIDLegacyNumeric (0.00s)
FAIL	github.com/cbl/cbl-webhook-ingress/internal/transport
```

The transport tests fail on the empty `GatewayID` (the numeric id and the
template are not resolved yet); the domain package lacks the new contract.

Green evidence (19.10.2026): `go test ./...` green; `go test -tags integration -count=1 ./internal/...`
green; `go test -tags e2e -count=1 ./test/e2e/` green (23.7 s); unit+integration
coverage 87.7%; `go vet` clean; golangci-lint (Docker, latest-alpine) 0 issues;
the em dash check passes.

Review closure (4 passes, 19.10.2026): findings closed in this commit - the
dangling doc comment removed, alternative templates supported (the event_key
grammar), the stale "without strict JSON parsing" wording fixed, the gateway_id
validation error taxonomy corrected (no ErrMissingEventKey leak), the BitPay
nested transport case, the camelCase legacy case and the oversized-id drop test
added, the .env.example CoinGate note moved to its own line, the AGENTS.md
trade-offs T-WH-09..T-WH-11 added and the NOWPayments mapping caveat recorded.
