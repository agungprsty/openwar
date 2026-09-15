import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  scenarios: {
    join_and_poll: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 1000 },
        { duration: '20s', target: 2000 },
        { duration: '10s', target: 0 },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.05'],
  },
};

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const EVENT = __ENV.EVENT || 'flash-sale-001';

export function setup() {
  return { token: 'demo-token' };
}

export default function (data) {
  // 1. Join the waiting room (establishes session cookie)
  const join = http.post(`${BASE}/event/${EVENT}/queue`, '{}', {
    headers: { Authorization: `Bearer ${data.token}` },
  });
  check(join, { 'join accepted': (r) => r.status === 202 || r.status === 201 });

  const sid = sidFromCookies(join.cookies);
  const cook = sid ? `openwar_session=${sid}` : '';

  // 2. Heartbeat in a mini poll loop
  const hb = http.post(`${BASE}/event/${EVENT}/queue/heartbeat`, '', {
    headers: { Cookie: cook },
  });

  let admitted = false;
  if (hb.status === 200) {
    admitted = hb.json('admitted') === true;
  }

  // 3. If admitted, attempt purchase with a fresh idempotency key
  if (admitted) {
    const key = crypto.randomUUID();
    const buy = http.post(
      `${BASE}/event/${EVENT}/purchase`,
      JSON.stringify({ sku: `${EVENT}:ticket`, qty: 1 }),
      {
        headers: {
          Cookie: cook,
          'Idempotency-Key': key,
          Authorization: `Bearer ${data.token}`,
        },
      },
    );
    check(buy, {
      'purchase accepted': (r) => r.status === 201 || r.status === 200,
      'purchase rejected on soldout': (r) => r.status !== 500,
    });
    sleep(1);
  }
}

function sidFromCookies(cookies) {
  if (cookies && cookies.openwar_session && cookies.openwar_session.length) {
    return cookies.openwar_session[0].value;
  }
  return '';
}