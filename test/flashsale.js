import http from 'k6/http';
import { check, sleep } from 'k6';
import crypto from 'k6/crypto';
import encoding from 'k6/encoding';
import { Counter, Trend } from 'k6/metrics';

// Custom metrics for flash sale analysis
const admittedTotal = new Counter('openwar_admitted_total');
const purchaseSuccess = new Counter('openwar_purchase_success');
const purchaseSoldout = new Counter('openwar_purchase_soldout');
const purchaseRateLimited = new Counter('openwar_purchase_rate_limited');
const purchaseOtherError = new Counter('openwar_purchase_other_error');
const queueWaitDuration = new Trend('openwar_queue_wait_duration_ms');

export const options = {
  scenarios: {
    flash_sale_surge: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '5s', target: 500 },
        { duration: '15s', target: 1500 },
        { duration: '10s', target: 500 },
        { duration: '5s', target: 0 },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.05'],
    'openwar_purchase_success': ['count>0'],
  },
};

const BASE = __ENV.BASE_URL || 'http://gateway:8080';
const EVENT = __ENV.EVENT || 'flash-sale-001';
const JWT_SECRET = __ENV.JWT_SECRET || 'docker-demo-secret';

function base64UrlEncode(str) {
  return encoding.b64encode(str, 'rawurl');
}

function generateJWT(secret, uid) {
  const header = JSON.stringify({ alg: 'HS256', typ: 'JWT' });
  const now = Math.floor(Date.now() / 1000);
  const payload = JSON.stringify({
    uid: uid,
    iat: now,
    exp: now + 86400,
  });

  const encodedHeader = base64UrlEncode(header);
  const encodedPayload = base64UrlEncode(payload);
  const tokenString = `${encodedHeader}.${encodedPayload}`;

  const sigBase64 = crypto.hmac('sha256', secret, tokenString, 'base64');
  const encodedSignature = sigBase64
    .replace(/=/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');

  return `${tokenString}.${encodedSignature}`;
}

function generateUUID() {
  const bytes = new Uint8Array(crypto.randomBytes(16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
  bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant
  let hex = '';
  for (let i = 0; i < 16; i++) {
    const h = bytes[i].toString(16).padStart(2, '0');
    hex += h;
  }
  return `${hex.substring(0, 8)}-${hex.substring(8, 12)}-${hex.substring(12, 16)}-${hex.substring(16, 20)}-${hex.substring(20, 32)}`;
}

export function setup() {
  return { secret: JWT_SECRET };
}

export default function (data) {
  const uid = `user-${__VU}-${__ITER}`;
  const token = generateJWT(data.secret, uid);

  // 1. Join the waiting room (establishes session cookie)
  const join = http.post(`${BASE}/event/${EVENT}/queue`, '{}', {
    headers: { Authorization: `Bearer ${token}` },
  });

  const joinOk = check(join, {
    'join accepted': (r) => r.status === 202 || r.status === 201,
  });
  let sid = '';
  try {
    const body = join.json();
    if (body && body.sessionID) {
      sid = body.sessionID;
    }
  } catch (_) {}
  if (!sid) {
    sid = sidFromCookies(join.cookies);
  }
  const cook = sid ? `openwar_session=${sid}` : '';
  const startTime = Date.now();

  // 2. Poll heartbeat until admitted or max retries reached
  let admitted = false;
  const maxHeartbeats = 25;

  for (let i = 0; i < maxHeartbeats; i++) {
    const hb = http.post(`${BASE}/event/${EVENT}/queue/heartbeat`, '', {
      headers: { Cookie: cook },
    });

    if (hb.status === 200) {
      const isAdm = hb.json('admitted') === true;
      if (isAdm) {
        admitted = true;
        break;
      }
    }

    // Wait 300ms between heartbeat polls
    sleep(0.3);
  }

  // 3. If admitted, proceed immediately to checkout
  if (admitted) {
    admittedTotal.add(1);
    queueWaitDuration.add(Date.now() - startTime);

    const idempotencyKey = generateUUID();
    const payload = JSON.stringify({ sku: `${EVENT}:ticket`, qty: 1 });

    const buy = http.post(
      `${BASE}/event/${EVENT}/purchase`,
      payload,
      {
        headers: {
          Cookie: cook,
          'Idempotency-Key': idempotencyKey,
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
      },
    );

    if (buy.status === 201 || buy.status === 200) {
      purchaseSuccess.add(1);

      // Sub-check: Idempotency verification replay (5% sample)
      if (Math.random() < 0.05) {
        const replay = http.post(
          `${BASE}/event/${EVENT}/purchase`,
          payload,
          {
            headers: {
              Cookie: cook,
              'Idempotency-Key': idempotencyKey,
              Authorization: `Bearer ${token}`,
              'Content-Type': 'application/json',
            },
          },
        );
        check(replay, {
          'idempotency replay returned cached response': (r) =>
            (r.status === 201 || r.status === 200) &&
            r.headers['X-Idempotent-Replay'] === 'true',
        });
      }
    } else if (buy.status === 410) {
      purchaseSoldout.add(1);
    } else if (buy.status === 429) {
      purchaseRateLimited.add(1);
    } else {
      purchaseOtherError.add(1);
    }

    check(buy, {
      'valid purchase response': (r) =>
        r.status === 201 || r.status === 200 || r.status === 410 || r.status === 429,
    });
  }
}

function sidFromCookies(cookies) {
  if (cookies && cookies.openwar_session && cookies.openwar_session.length) {
    return cookies.openwar_session[0].value;
  }
  return '';
}

export function handleSummary(data) {
  const dateStr = new Date().toISOString().replace(/[:.]/g, '-');
  const filepath = `/scripts/result_${dateStr}.json`;
  return {
    [filepath]: JSON.stringify(data, null, 2),
    stdout: `\n=== FLASH SALE LOAD TEST SUMMARY ===\n` +
      `Checks Succeeded: ${data.metrics.checks ? data.metrics.checks.values.rate * 100 : 0}%\n` +
      `Admitted Users:   ${data.metrics.openwar_admitted_total ? data.metrics.openwar_admitted_total.values.count : 0}\n` +
      `Purchases Won:    ${data.metrics.openwar_purchase_success ? data.metrics.openwar_purchase_success.values.count : 0}\n` +
      `Purchases Soldout:${data.metrics.openwar_purchase_soldout ? data.metrics.openwar_purchase_soldout.values.count : 0}\n` +
      `Rate Limited:     ${data.metrics.openwar_purchase_rate_limited ? data.metrics.openwar_purchase_rate_limited.values.count : 0}\n` +
      `Avg Duration:     ${data.metrics.http_req_duration ? data.metrics.http_req_duration.values.avg.toFixed(2) : 0}ms\n` +
      `=====================================\n`,
  };
}