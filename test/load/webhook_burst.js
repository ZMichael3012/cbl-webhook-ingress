// Load scenario CBL-5.4: webhook burst and PostgreSQL degradation.
// Thresholds: response p99 below 500 ms, no 5xx (CBL-002, section 8).
//
// Commands (cmd.exe, from the services/cbl-webhook-ingress directory):
//   docker compose -f deploy/docker-compose.yml --profile app up -d --build
//   docker run --rm -i -v "%CD%/test/load":/scripts grafana/k6:2.3.0 run /scripts/webhook_burst.js
// Degraded profile (503 + Retry-After with PostgreSQL paused):
//   docker compose -f deploy/docker-compose.yml pause postgres
//   docker run --rm -i -v "%CD%/test/load":/scripts -e PROFILE=degraded grafana/k6:2.3.0 run /scripts/webhook_burst.js
//   docker compose -f deploy/docker-compose.yml unpause postgres
// PowerShell: the same commands with %CD% replaced by $PWD (for example, -v "$PWD/test/load":/scripts).
import http from 'k6/http';
import crypto from 'k6/crypto';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const profile = (__ENV.PROFILE || 'burst').toLowerCase();
const url = __ENV.WEBHOOK_URL || 'http://host.docker.internal:8060/hooks/mock/ep1';
const secret = __ENV.WEBHOOK_SECRET || 'mock-secret';
const burstRate = Number(__ENV.RATE || 500);
const degradedRate = Number(__ENV.DEGRADED_RATE || 20);
// Unique run prefix: a repeated run is not a replay (HMAC window +/- 5 minutes).
const runID = __ENV.RUN_ID || `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;

// server_errors records 5xx responses (expected 503s in the degraded profile are visible separately),
// rate_limited - intake limit hits (there must be none in the burst profile),
// unexpected_responses - responses that do not match the profile expectation.
const serverErrors = new Counter('server_errors');
const rateLimited = new Counter('rate_limited');
const unexpectedResponses = new Counter('unexpected_responses');

export const options = profile === 'degraded'
  ? {
      summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
      scenarios: {
        db_degraded: {
          executor: 'constant-arrival-rate',
          rate: degradedRate,
          timeUnit: '1s',
          duration: '15s',
          preAllocatedVUs: 20,
          maxVUs: 60,
        },
      },
      thresholds: {
        checks: ['rate>0.999'],
        unexpected_responses: ['count==0'],
        http_req_duration: ['p(99)<1000'],
        dropped_iterations: ['count<3'],
      },
    }
  : {
      summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
      scenarios: {
        webhook_burst: {
          executor: 'ramping-arrival-rate',
          startRate: 50,
          timeUnit: '1s',
          stages: [
            { target: burstRate, duration: '10s' },
            { target: burstRate, duration: '30s' },
            { target: 50, duration: '5s' },
          ],
          preAllocatedVUs: 100,
          maxVUs: Math.max(500, burstRate + 100),
        },
      },
      thresholds: {
        http_req_duration: ['p(99)<500'],
        checks: ['rate>0.999'],
        http_req_failed: ['rate<0.001'],
        server_errors: ['count==0'],
        rate_limited: ['count==0'],
        unexpected_responses: ['count==0'],
        dropped_iterations: ['count<192'],
      },
    };

export default function () {
  const eventID = `${runID}-${__VU}-${__ITER}`;
  const body = `{"gateway_id":"g-${eventID}","status":"detected"}`;
  const timestamp = Math.floor(Date.now() / 1000);
  const signature = crypto.hmac('sha256', secret, `${timestamp}.${body}`, 'hex');
  const response = http.post(url, body, {
    headers: {
      'Content-Type': 'application/json',
      'X-Event-Id': eventID,
      'X-Billing-Signature': `t=${timestamp},v1=${signature}`,
    },
  });

  if (response.status >= 500) {
    serverErrors.add(1);
  }
  if (response.status === 429) {
    rateLimited.add(1);
  }
  let expected = false;
  if (profile === 'degraded') {
    expected = response.status === 503
      && response.headers['Retry-After'] === '1'
      && response.body.includes('write_timeout');
  } else if (response.status === 200) {
    const payload = response.json();
    expected = payload !== null && payload.status === 'accepted' && payload.duplicate === false;
  }
  if (!expected) {
    unexpectedResponses.add(1);
  }
  check(response, {
    'expected response': () => expected,
  });
}
