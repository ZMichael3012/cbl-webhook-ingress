# CBL-5.3. Protection: rate limits and gateway IP allowlist

| Field | Value |
| :-- | :-- |
| Epic | E5, Webhook Ingress |
| Repository | `cbl-webhook-ingress` |
| Size | S |
| Wave and parallel group | W1-WEBHOOK |
| Depends on | 5.1, 5.2 |
| Contract version | `cbl-protos@cf-1`, `CBL-003` sections 4, 7 |
| Status | done, review closed |

## 1. Context and goal

Webhook Ingress is an internet-facing service: webhooks arrive directly from the gateway,
bypassing the API Gateway perimeter (`CBL-000`, section 2). This task adds network
protection: an application-level allowlist of gateway addresses and per-provider intake
rate limits, and records the contract of a separate deployment (its own workload, network
isolation).

## 2. Boundaries

In scope: provider `ip_allowlist` (CIDR), rejection with `403`; per-provider intake limit
(token bucket, rejection with `429` plus `Retry-After`); refusal to trust proxy headers;
recording of the separate deployment.

Out of scope: TLS termination and mTLS (0.6), K8s NetworkPolicy (0.3), perimeter WAF,
rate limit metrics (0.7), HMAC (5.1), write budget (5.2).

## 3. Functional requirements

FR-1. The provider configuration is extended with `WEBHOOK_PROVIDERS` fields:
`ip_allowlist` - an array of CIDRs (`10.0.0.0/8`) or single addresses (treated as /32,
/128); an empty list allows any address (dev mode, a warning
`provider ip allowlist is empty` is written to the log); an invalid element means the
service does not start. `rate_rps` - the sustained intake rate per second (0 - no limit),
`rate_burst` - the burst capacity (0 - normalized to `rate_rps`). Limits: up to 64
allowlist entries, `rate_rps` 1 - 100 000, `rate_burst` 1 - 1 000 000; a violation is a
configuration error. `rate_burst` with `rate_rps = 0` is an error (the limit is defined as
a pair).

FR-2. Address check: the TCP peer address (`RemoteAddr`) is compared, not the
`X-Forwarded-For`/`X-Real-IP` headers: proxy headers must not be trusted on direct intake
(address spoofing). The check is performed after provider resolution and before reading
the body; a rejection is `403 application/problem+json` without details. An unparsable
`RemoteAddr` with a configured allowlist is a rejection (fail-closed).

FR-3. Per-provider intake limit: a thread-safe token bucket with lazy refill (no
background goroutines): sustained rate `rate_rps`, burst `rate_burst`. One request
consumes one token; on exhaustion - `429 application/problem+json` with `Retry-After: 1`
before reading the body and verifying the signature. The limit is in-process: the
critical write path does not depend on Redis (T-02, T-11); with several replicas the
limit is approximate (aggregate protection is the perimeter, 7.5).

FR-4. Order of checks in the handler: provider (`404`) -> allowlist (`403`) ->
endpoint (`404`) -> limit (`429`) -> body size (`413`) -> signature (`401`) ->
event id (`422`) -> write (`503`/`408`/`500`/`200`). The allowlist comes before the
endpoint check so that a foreign address cannot tell an existing endpoint from a
nonexistent one.

FR-5. Separate deployment: the service is deployed as a separate workload (locally a
separate compose service, in prod a separate Deployment, template 0.3) and accepts
gateway traffic directly; network access is restricted by the application allowlist, and
at the perimeter by NetworkPolicy/firewall over the gateway addresses (tasks 0.3, 0.6).
The public listeners `/healthz`, `/readyz` (8060) and `/metrics` (9260) are not exposed
outward; their isolation is part of template 0.3. The service is not placed behind the
shared merchant rate limit circuit: gateway traffic does not pass the API Gateway
perimeter (7.5), so aggregate protection is network-level (0.3).

## 4. Contracts

The HTTP contract is extended with `403` and `429` (both `application/problem+json`);
`429` carries `Retry-After: 1`. The envelope and topics do not change. The configuration
is an extension of `WEBHOOK_PROVIDERS` (backward compatible: the new fields are
optional).

## 5. Data model and migrations

No new tables or migrations: the limits and the allowlist are process configuration.

## 6. State machines

Not applicable.

## 7. Edge cases

Empty allowlist (dev, warning in the log); IPv4, IPv6 and single addresses; a top-level
subnet `0.0.0.0/0` (allows all IPv4 addresses; a separate `::/0` is needed for IPv6);
IPv4-mapped subnets (`::ffff:a.b.c.d/nn`) are normalized to IPv4; `RemoteAddr` without a
port, with an IPv6 zone (rejection) or unparsable (rejection); spoofing of
`X-Forwarded-For` with a forbidden peer (does not help); `rate_rps=0` (limit disabled);
`rate_burst=0` (normalized to rps); burst exhaustion; token refill over time; concurrent
requests to the bucket; limit and allowlist together; a request rejected before reading
the body and before signature verification (tests: an oversized body and a missing
signature rejected by address/limit do not produce `413`/`401`).

## 8. Non-functional requirements

The checks are cheap (O(1), no per-request allocations) and do not affect the 500 ms
budget; the bucket does not create goroutines; the limit does not depend on external
services; 429/403 do not write the raw body or the client address into problem+json (the
address is allowed in warn-level logs).

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Unit (domain) | parsing of `ip_allowlist` (IPv4/IPv6/single/garbage/limit 64), `rate_rps`/`rate_burst` ranges, default burst; limiter: burst, refill over time, disabled, concurrency |
| Unit (transport) | `403` for a foreign IPv4/IPv6, `200` for an allowed one, ignoring of `X-Forwarded-For`, fail-closed on a garbage `RemoteAddr`, `429` plus `Retry-After` after the burst, recovery after refill, order of checks (403/429 before reading the body) |
| Integration | existing scenarios pass with the allowlist enabled (the localhost address of the testcontainers client is not involved: the transport is httptest) |
| E2E | unchanged (5.1) |
| Load | 5.4: the limit is configured above the profile |

## 10. Work plan and commit

Tests first (domain, limiter, transport), then the implementation: config fields and
validation, `internal/domain/limiter.go`, checks in `internal/transport/server.go`,
`SetTrustedProxies(nil)` in main. Commit `CBL-5.3: allowlist IP and webhook rate limits`;
the spec is in the same commit.

## 11. Open questions

- The aggregate limit with several replicas is not supported (in-process); the decision
  was made to keep Redis off the critical path (T-02). Aggregate protection is
  network-level (0.3: NetworkPolicy/firewall, LB limits); the API Gateway perimeter (7.5)
  does not see webhooks and must not be relied on. Revisit when a multi-replica
  requirement appears.
- A bucket reset on restart/deploy gives a brief legitimate burst above `rate_rps`; this
  is accepted (the limit is protection against brute force, not billing).
- Tokens are consumed before signature verification: with an empty allowlist a scanner
  can exhaust the bucket and cause `429` for real deliveries until the window expires; in
  prod the allowlist is mandatory, and the threshold is verified by load test 5.4.
- Behind a proxy/load balancer the peer address will be the proxy address: the allowlist
  is filled with proxy addresses and the real gateway address is not distinguished; the
  solution is network restrictions (0.3) and transport-level access restriction to the
  gateway (0.6, KI-webhook-ingress-04).

## 12. Review (CBL-001, step 6)

Four independent passes (14.10.2026). No blockers. The common MAJOR of all roles (the
warning for an empty allowlist was not implemented) was closed by
`domain.MissingAllowlist` and a warning at startup; the architecture MAJOR (incorrect
compensation "aggregate limit is perimeter 7.5") was closed by revising T-WH-06, FR-5
and section 11; the remaining findings were closed by tests, the order of checks and
documentation clarifications.

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | no warning for an empty allowlist; the order of checks is not proven by tests; no refill test at the transport level; integration without the allowlist; strictness of `rate_burst` without `rate_rps`; `0.0.0.0/0` and IPv6; the journal runs ahead of the process; the boundaries 64/100000/1000000 are not covered by tests; `RemoteAddr` without a port | the warning is implemented and covered by `MissingAllowlist`; tests "403/429 before the body and the signature"; a refill test with controlled clocks; `TestIntegrationAllowlistActive`; the rule is added to FR-1; a clarification about `::/0`; finalization by this block; boundary tests; the no-port case and the IPv6 zone in the table |
| Architecture | MAJOR: the compensation of the aggregate limit referenced 7.5 (the perimeter does not see webhooks); enumeration through 404/403; health/metrics outside FR-5; mTLS 0.6 is inter-service only; the test plan is wider than the implementation; rate_burst is normalized only in the limiter | T-WH-06/FR-5/section 11 rewritten (aggregate protection is network-level 0.3); the allowlist moved before the endpoint; FR-5 extended with the isolation of health/metrics; T-WH-05/KI-04 clarified; tests added; normalization in the config |
| With context | MAJOR: the warning for an empty allowlist is missing; the journal runs ahead of the review; a mapped CIDR becomes a dead entry; the boundaries are not covered by tests; the test plan is wider than the implementation; formatting; `SetTrustedProxies(nil)` against the code; dependency 5.3 in CBL-004 | the warning is implemented; finalization; mapped subnets are normalized (`normalizePrefix`); boundary tests; tests added; empty line; `nil` in main; CBL-004 updated |
| Security | MAJOR: the warning is missing (fail-open dev mode is unnoticeable); tokens are consumed before the signature (scanners exhaust the bucket); mapped CIDR; the no-port/zone cases are not protected by tests; a double warn log on 403 | the warning is implemented; the risk is recorded in section 11 and T-WH-06 (the allowlist is mandatory in prod); prefix normalization; tests added; log sampling is 0.7 |
