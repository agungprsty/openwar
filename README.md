# OpenWar — Token Bucket & Virtual Waiting Room

![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green)
![Docker](https://img.shields.io/badge/docker-ready-2496ED?logo=docker&logoColor=white)

> High-concurrency demand shaping for war tickets, flash sales, giveaways, and any
> limited-quantity give-away war. A custom **API Gateway** (Go, stdlib `net/http`)
> with a **Virtual Waiting Room** middleware and a distributed **Token Bucket**
> rate limiter backed by **Redis Lua scripts**, decoupled through **NATS JetStream**,
> fully containerized with **Docker Compose**.
>
> Production-hardened with four enhancements: **sharded inventory counters** (hot-key
> mitigation), **client-generated idempotency keys** (double-submit prevention),
> **heartbeat liveness** (zombie session eviction), and **NATS failure compensation**
> (atomic stock restore).

---

## Table of Contents

- [Why This Exists](#why-this-exists)
- [Architecture](#architecture)
- [Core Concepts](#core-concepts)
  - [Token Bucket Algorithm](#token-bucket-algorithm)
  - [Inventory Partitioning (Hot-Key Mitigation)](#inventory-partitioning-hot-key-mitigation)
  - [Virtual Waiting Room & Heartbeat Liveness](#virtual-waiting-room--heartbeat-liveness)
  - [Client-Generated Idempotency Key](#client-generated-idempotency-key)
  - [NATS JetStream & Compensation](#nats-jetstream--compensation)
- [Payment Window & Stock Rollback (15-Minute Timeout)](#payment-window--stock-rollback-15-minute-timeout)
- [Custom API Gateway & Middleware Chain](#custom-api-gateway--middleware-chain)
- [Repository Layout](#repository-layout)
- [Getting Started (Docker)](#getting-started-docker)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Load Testing & Monitoring](#load-testing--monitoring)
- [Roadmap](#roadmap)

---

## Why This Exists

When 50,000 users hammer a single endpoint at the same second — a concert ticket
war, a limited sneaker drop, a flash sale — the naive approach collapses:

- The database row-lock queue saturates the connection pool in milliseconds.
- "Fixed window" rate limiters let a 2x burst slip through at window boundaries.
- In-memory limiters diverge across gateway instances.
- One Redis SKU counter becomes a hot-key that saturates a single cluster slot.
- Browsers double-fire checkout, and abandoned queue sessions never get cleaned up.

**OpenWar** solves this with *demand shaping*: instead of letting 50K requests
reach the backend at once, it serializes them through layered gates that each drop
traffic by an order of magnitude. The backend only ever sees a steady,
sustainable stream.

```
50,000 requests/second
      │
      ▼
┌─────────────────────────────┐
│  CDN / static sale page     │  absorbs ~90% of page loads
└─────────────────────────────┘
      │ ~5,000 rps
      ▼
┌─────────────────────────────┐
│  Virtual Waiting Room        │  Redis sorted set (FIFO by timestamp)
│  join + poll /queue/status   │  heartbeat evicts zombies every 15s
└─────────────────────────────┘
      │ ~1,000 rps admitted    │  admission worker (ZPOPMIN, live-only)
      ▼
┌─────────────────────────────┐
│  Token Bucket (Redis Lua)    │  distributes burst capacity
└─────────────────────────────┘
      │ backend capacity rps
      ▼
┌─────────────────────────────┐
│  Inventory Gate (Redis Lua) │  sharded atomic DECR — oversell-proof
│  + idempotency key check    │  duplicate submits rejected
└─────────────────────────────┘
      │
      ▼
┌─────────────────────────────┐
│  Order Queue (NATS JetStream)│  async, durable, at-least-once
└─────────────────────────────┘    │ publish fails  → compensation (INCR)
      │                            ▼                     │
      ▼                     ┌─────────────────────┐  ┌────────────────────┐
   Backend Services         │  Reconciler / DLQ   │  │  Compensator        │
                            │  worker             │  │  restore stock      │
                            └─────────────────────┘  └────────────────────┘
```

---

## Architecture

```mermaid
flowchart TB
    U["Users / Bots"] --> CDN["CDN / Static Page"]
    CDN --> GW["API Gateway (Go net/http)"]

    subgraph GW["API Gateway — middleware chain"]
        direction TB
        M1[Recovery] --> M2[RequestID]
        M2 --> M3[Structured Logger]
        M3 --> M4[JWT Auth]
        M4 --> WRM["Waiting Room middleware<br/>(admission token check)"]
        WRM --> WRI["Heartbeat / queue middleware"]
        WRI --> TBM["Token Bucket middleware (Redis Lua)"]
        TBM --> IDM["Idempotency middleware<br/>(UUID v4 claim)"]
        IDM --> RP["httputil.ReverseProxy"]
    end

    RP --> BE["Backend Service (demo)"]

    WRM <--> R1[("Redis — room:&lt;e&gt;:queue + hb + admitted")]
    TBM <--> R2[("Redis — bucket:&lt;key&gt; hash")]
    IDM <--> R3[("Redis — idem:&lt;key&gt; hash")]
    BE --> INV[("Redis — inventory:&lt;sku&gt;:shard:&lt;i&gt;")]

    BE --> JS["NATS JetStream"]
    JS --> W1["Order Worker (durable consumer)"]
    W1 --> DB[("Order DB")]
    JS --> W2["Queue Notifier<br/>SSE position updates"]
    JS --> DLQ["orders_created.DLQ"]
    DLQ --> RC["Reconciler → Compensator (INCR stock)"]
```

### Why each layer

| Layer | Problem it solves | Cost it pays |
|---|---|---|
| **CDN / static page** | Snapshot load, bot page-refresh | Stale content |
| **Virtual Waiting Room** | Fairness; no thundering herd | User-facing wait + polling tax |
| **Heartbeat liveness** | Zombie sessions land at queue front | 1 extra key TTL per session |
| **Token Bucket** | Clamps sustained traffic to capacity | Slight burst above refill rate |
| **Sharded inventory (Lua)** | Overselling prevention + hot-key mitigation | Statistically even but uneven-tails depletion |
| **Idempotency key** | Double-submit / retry replay | One Lua claim + cached response per purchase |
| **NATS JetStream** | Decouples checkout from durable order write | Eventual consistency |
| **Compensator** | Restores stock when publish/insert fails | State machine + DLQ reconciler |

---

## Core Concepts

### Token Bucket Algorithm

A bucket holds up to `capacity` tokens. Tokens refill continuously at a
`refill_rate` (tokens/sec) up to the capacity. Each request consumes one token.

- A **burst** is allowed up to the capacity (e.g. a browser firing 5 requests
  on page load).
- **Steady state** is bounded by the refill rate — the backend can never sustain
  more than `refill_rate` requests/sec per key.
- No clock boundary reset, so it **defeats the fixed-window "2x boundary burst"
  exploit** that plagues `INCR` counters right at the sale start (09:59:59 →
  10:00:00).

In an in-memory implementation this is just two numbers per bucket — but across
multiple gateway instances you need a shared store. OpenWar keeps the bucket
state in **Redis** and runs the refill+consume atomically inside a **Lua script**
so competing gateway nodes can never race:

```lua
-- KEYS[1] = bucket key, e.g. "{event:1001}:bucket"
-- ARGV[1] = capacity, ARGV[2] = refill_rate (tokens/sec)
-- ARGV[3] = now (float unix sec), ARGV[4] = requested tokens (usually 1)

local data   = redis.call('HMGET', KEYS[1], 'tokens', 'last_updated')
local tokens = tonumber(data[1])
local last   = tonumber(data[2])

if tokens == nil or last == nil then
    tokens = capacity
    last   = now
else
    tokens = math.min(capacity, tokens + math.max(0, now - last) * refill_rate)
end

local ttl = math.ceil(capacity / refill_rate) * 2   -- auto-evict idle buckets

if tokens >= requested then
    tokens = tokens - requested
    redis.call('HSET', KEYS[1], 'tokens', tokens, 'last_updated', now)
    redis.call('EXPIRE', KEYS[1], ttl)
    return {1, math.floor(tokens), 0}               -- allowed, remaining, retry_after
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'last_updated', now)
redis.call('EXPIRE', KEYS[1], ttl)
local retry_after = math.ceil((requested - tokens) / refill_rate)
return {0, math.floor(tokens), retry_after}         -- denied, remaining, retry_after
```

Calling it from Go (cache the SHA to avoid re-sending the body):

```go
script := redis.NewScript(tokenBucketLua)

res, err := script.Run(ctx, rdb, []string{bucketKey},
    capacity, refillRate, now, 1).Result()
// → [1, remaining, 0] if allowed, [0, remaining, retryAfter] if denied
```

> **Redis cluster tip:** always key buckets by `{hash_tag}` so the Lua script
> executes on a single slot and avoids `CROSSSLOT` errors.

**Fail strategy** (configurable per route):

- **Fail-open** for authenticated VIP traffic — unknown load is shaped downstream
  by NATS backpressure instead.
- **Fail-closed** for unauthenticated / bot-tagged IP ranges.

---

### Inventory Partitioning (Hot-Key Mitigation)

**The problem.** In cluster mode every `DECR inventory:sku` routes to the same
slot, so one master saturates while the rest idle — even before the DB is touched.

**The scheme.** Split stock into **N shard counters** and diffuse contention:

```
inventory:iphone-15:shard:0   -> 3,125
inventory:iphone-15:shard:1   -> 3,125
...
inventory:iphone-15:shard:31  -> 3,125   (total = 100,000 units)
```

Purchases pick a home shard deterministically by `userID` (same user → same
shard, contention spread evenly), `DECR` atomically, and probe sibling shards
when one is exhausted. Pre-seed at sale setup:

```go
func (s *Seeder) SeedPartitionedStock(ctx context.Context, sku string, stock int64, shards int) error {
    base := stock / int64(shards)
    rem := stock % int64(shards) // spread remainder so sum(shards) == stock
    for i := 0; i < shards; i++ {
        qty := base
        if int64(i) < rem { qty++ }
        if err := s.rdb.Set(ctx, fmt.Sprintf("inventory:%s:shard:%d", sku, i), qty, 0).Err(); err != nil {
            return err
        }
    }
    return s.rdb.Del(ctx, "soldout:"+sku).Err() // clear sellout latch on re-run
}
```

`internal/lua/reserve_stock.lua` — atomically decrement **one** shard:

```lua
-- KEYS[1] = inventory:<sku>:shard:<i>
-- Returns {1, remaining} on success, {0, -1} when this shard is empty.
local remaining = redis.call('DECR', KEYS[1])
if remaining < 0 then
    redis.call('INCR', KEYS[1]) -- refund over-decrement; shard exhausted
    return {0, -1}
end
return {1, remaining}
```

Go shard router with **bounded fallback** + sellout latch:

```go
func (r *ShardRouter) Reserve(ctx context.Context, sku, userID string) (string, error) {
    home := shardFor(userID, r.shards) // fnv32a(userID) % shards

    if r.isSoldOut(ctx, sku) { return "", ErrSoldOut } // fast-path latch

    for offset := 0; offset < r.shards; offset++ {     // at most one full sweep
        key := fmt.Sprintf("inventory:%s:shard:%d", sku, (home+offset)%r.shards)
        res, err := reserveScript.Run(ctx, r.rdb, []string{key}).Int64Slice()
        if err != nil { continue }                     // Redis hiccup → try another shard
        if res[0] == 1 { return key, nil }             // reserved; key needed for §compensation
    }
    r.latchSoldOut(ctx, sku)                           // every shard empty → latch gate
    return "", ErrSoldOut
}
```

> **Cluster caveat:** shard keys must **not** share a hash tag
> (`inventory:{sku}:shard:{i}` would re-pin all shards to one slot and recreate
> the hot-key). `reserve_stock.lua` is single-key, so it is `CROSSSLOT`-safe.
>
> **Alternative for very large sales:** pre-mint one token per unit into per-shard
> lists and purchase by `LPOP` (O(1), "winners bounded by list length"). Trade-off:
> no exact remaining-count introspection.

---

### Virtual Waiting Room & Heartbeat Liveness

**Queue & fairness.** A **Redis sorted set scored by arrival timestamp**:

```text
ZADD room:{eventID}:queue NX <unix-nano> <sessionID>   -- join, dedupe
ZRANK room:{eventID}:queue <sessionID>                  -- live position
ZPOPMIN room:{eventID}:queue N                           -- admit next N (FIFO)
```

An **admission worker** ticks on an interval and admits the next `N` live
sessions based on *backend capacity*, not request rate:

1. `ZPOPMIN` the oldest `N` **live** sessions.
2. Issue a short-lived (default 5-minute) **admission token** —
   `room:<event>:admitted:<sessionID>` `SET … EX`.
3. Publish `queue.notify` to NATS so pollers (or SSE/WebSocket) learn they were
   admitted.

**Zombie problem & heartbeat.** Users close the tab after joining; their
`sessionID` stays in the queue forever and wastes admission tokens. OpenWar adds
a **liveness contract**:

- The queue page pings `POST /event/{id}/queue/heartbeat` **every 15s**.
- Each ping refreshes `room:<event>:hb:<sessionID>` with **TTL 45s** (3× the
  interval, absorbing jitter).
- The admission worker **never admits a member whose heartbeat is missing** —
  zombies are dropped and their slot reused within the same batch.

`internal/lua/admit_batch.lua` — FIFO admit with zombie eviction:

```lua
-- KEYS[1] = room:<event>:queue (zset)
-- ARGV[1] = "room:<event>:hb:"  heartbeat key prefix
-- ARGV[2] = "room:<event>:admitted:" admission key prefix
-- ARGV[3] = max admissions, ARGV[4] = admission TTL seconds
local admitted, out = 0, {}
while admitted < tonumber(ARGV[3]) do
    local top = redis.call('ZPOPMIN', KEYS[1], 1)
    if not top[1] then break end              -- queue drained
    local sid = top[1]
    if redis.call('EXISTS', ARGV[1] .. sid) == 1 then
        redis.call('SET', ARGV[2] .. sid, '1', 'EX', tonumber(ARGV[4]))
        admitted = admitted + 1
        out[#out + 1] = sid
    end
    -- zombies: ZPOPMIN already removed them; slot reused in this batch
end
return out
```

Heartbeat handler (fresh liveness + live position in one round-trip):

```go
func (rm *Room) Heartbeat(ctx, event, sid) (position int64, admitted bool, err error) {
    pipe := rm.rdb.TxPipeline()
    pipe.Set(ctx, hbKey(event, sid), 1, 45*time.Second)  // refresh liveness
    rank := pipe.ZRank(ctx, queueKey(event), sid)          // FIFO position
    adm  := pipe.Exists(ctx, admitKey(event, sid))
    if _, err = pipe.Exec(ctx); err != nil { return -1, false, err }
    return rank.Val(), adm.Val() == 1, nil
}
```

Client polling (embedded in the queue page):

```js
const poll = async () => {
  const res = await fetch(`/event/${EVENT_ID}/queue/heartbeat`, {
    method: 'POST', credentials: 'include' });
  const st = await res.json();
  document.querySelector('#pos').textContent = st.position;
  if (st.admitted) location.href = `/event/${EVENT_ID}/checkout`;
};
setInterval(poll, 15_000); poll();
```

The checkout endpoint rejects any request **without a valid, unexpired,
single-use admission token** — this stops token-sharing and replay.

> Optional janitor: a periodic `SCAN room:<e>:hb:*` sweep can `ZREM` expired
> members so `ZCARD` (dashboard, autoscaling) reflects only live users. The lazy
> eviction above already guarantees correctness of admits.

---

### Client-Generated Idempotency Key

**The problem.** A browser double-fires `POST /purchase` (double-click, retry
middleware, connection retransmission). A per-request check can't tell "user
clicked twice" from "two separate purchases".

**The fix.** The client generates **one UUID v4 per checkout attempt**, sent as
the `Idempotency-Key` header. The gateway admits only the **first** request
carrying that key; every replay gets a cached response or a conflict.

```
Browser                      Gateway                              Redis
   POST /purchase  Idem-Key: a1b2…  │                                │
   ─────────────────────────────────▶│  claim_idem.lua  (atomic)     │
                                     │──────────────────────────────▶│
                                     │◀── {1, ACCEPTED}              │  first time
                                     │  → proxy → backend → ok       │
   POST /purchase  Idem-Key: a1b2…  │                                │
   ─────────────────────────────────▶│  claim_idem.lua  (EXISTS)     │
                                     │◀── {0, DUPLICATE}             │  replay
   ◀──────── 409 + cached response ──│                                │
```

`internal/lua/claim_idem.lua` — atomic, race-free claim:

```lua
-- KEYS[1] = idem:<userID>:<eventID>:<idemKey>
-- ARGV[1] = TTL s, ARGV[2] = request fingerprint, ARGV[3] = unix ms
local data = redis.call('HMGET', KEYS[1], 'status', 'fingerprint')
if data[1] ~= false then
    if data[2] ~= ARGV[2] then return {0, 'MISMATCH'} end -- same key, diff body
    return {0, 'DUPLICATE'}                                -- replay → never re-run
end
redis.call('HSET', KEYS[1], 'status', 'PENDING', 'fingerprint', ARGV[2], 'createdAt', ARGV[3])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1]))
return {1, 'ACCEPTED'}
```

> Why Lua instead of `SETNX`? `SETNX` alone can't detect "same key, different
> payload". The script compares the request fingerprint atomically with the
> claim — two racing requests cannot both pass the EXISTS check.

Gateway middleware (validates **strict UUID v4**, scoped per user+event):

```go
var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// inside middleware, after reading the body into a buffer for fingerprinting:
switch res[1] { // res = claimScript.Run(...) → StringSlice
case "DUPLICATE":
    if snap, _ := rdb.HGetAll(ctx, rkey).Result(); snap["respBody"] != "" {
        code, _ := strconv.Atoi(snap["statusCode"])
        w.Header().Set("X-Idempotent-Replay", "true")
        w.WriteHeader(code); w.Write([]byte(snap["respBody"]))   // replay cached result
        return
    }
    writeJSON(w, http.StatusConflict, map[string]string{"error": "duplicate request"})
    return
case "MISMATCH":
    writeJSON(w, http.StatusUnprocessableEntity, map[string]string{
        "error": "Idempotency-Key reused with a different payload"})
    return
}
// 1st time → stamp the key into context, set r.Body = io.NopCloser(buf), proxy
```

After the upstream completes, a wrapped `ResponseWriter` persists
`{status, statusCode, respBody}` under the same key (TTL refreshed to 24h). If
the upstream returned **5xx (nothing committed)** the record is marked `FAILED`
so the *same* key is retryable — no false "duplicate".

| Scenario | Result |
|---|---|
| First request, key unknown | `201/200`, executes once |
| Same key + same body replayed | `409` + cached response (`X-Idempotent-Replay: true`) |
| Same key + **different** body | `422 Unprocessable Entity` |
| Upstream 5xx (no commit) | `FAILED` recorded; key retryable |
| Redis down during claim | fail-open, decision left to upstream |

---

### NATS JetStream & Compensation

**Event decoupling.** NATS separates the hot request path from the durable write
path (below retention limits policy, replayable).

| Subject | Emitter | Consumer | Purpose |
|---|---|---|---|
| `orders.created` | Backend after shard DECR | Order Worker | Async order persistence |
| `payment.processed` | Order Worker | Notifier | DB write-offload, idempotent |
| `stock.updated` | Worker | Inventory projection | Eventual-consistency stock views |
| `queue.notify` | Admission worker | Gateway | Real-time queue position → poller |

Durable consumer (at-least-once, replays on crash):

```go
js, _ := nc.JetStream()
sub, _ := js.SubscribeSync("orders.created",
    nats.Durable("order-processor"),
    nats.AckWait(30*time.Second),
    nats.ManualAck(),
)
msg, _ := sub.NextMsg(5*time.Second)
handleOrder(msg)   // idempotent — unique key (orderID) in DB
msg.Ack()          // ack only after successful insert
```

**Failure compensation.** The DECR and the NATS publish cannot be atomic
together. If the publish fails (NATS down, timeout, nil PubAck), the reserved
stock must be **restored**. A per-purchase **reservation state machine** makes
compensation idempotent — two retriers can never double-`INCR`:

```
             reserve_stock.lua
                    │
                    ▼
             RESERVED ──publish ok──► PUBLISHED ──worker insert ok──► COMPLETED
                 │
                 │ publish error / timeout / abandoned
                 ▼
             (COMPENSATING) ──INCR──► COMPENSATED
```

`internal/lua/release_stock.lua` — guard + restore in one atomic step:

```lua
-- KEYS[1] = reservation:<orderID>
-- ARGV[1] = inventory shard key, ARGV[2] = qty, ARGV[3] = reason
-- Returns {1, remaining} when restored, {0, state} when already handled.
local st = redis.call('GET', KEYS[1])
if not st then return {0, 'NO_RESERVATION'} end
if st ~= 'RESERVED' then return {0, st} end   -- already PUBLISHED/COMPENSATED
redis.call('SET', KEYS[1], 'COMPENSATING')     -- atomic claim first
local remaining = redis.call('INCR', ARGV[1])  -- refund (node-local; see caveat)
redis.call('SET', KEYS[1], 'COMPENSATED')
return {1, remaining}
```

Go: publish-with-compensation:

```go
func (s *Service) Place(ctx context.Context, order Order) error {
    orderID := genOrderID(ctx)

    // 1. reserve on a shard; returns the winning key for exact refund (§inventory)
    invKey, err := s.rr.Reserve(ctx, order.Sku, order.UserID)
    if err != nil { return ErrSoldOut }

    // 2. mark reservation RESERVED (TTL auto-expires abandoned holds)
    resKey := "reservation:" + orderID
    if err := s.rdb.Set(ctx, resKey, "RESERVED", 25*time.Minute).Err(); err != nil {
        s.compensate(ctx, resKey, invKey, 1, "redis_down_after_reserve")
        return err
    }

    // 3. publish durable event (nil PubAck ⇒ no stream stored it ⇒ failure)
    payload, _ := json.Marshal(orderEvent{OrderID: orderID, Sku: order.Sku, ShardKey: invKey})
    if err := s.publish(ctx, payload); err != nil {
        // 4. COMPENSATE — the Lua guard prevents a double-INCR
        if _, cErr := s.compensate(ctx, resKey, invKey, 1, "nats_publish_failed"); cErr != nil {
            // guard said already PUBLISHED → let the worker reconcile; alert here
        }
        return fmt.Errorf("publish: %w", err) // client retries with same idem key
    }

    // 5. success path; worker flips RESERVED → PUBLISHED → COMPLETED
    return nil
}
```

**Cluster-mode caveat.** `release_stock.lua` runs `INCR` on a key that may live
in a different slot than `KEYS[1]`. Safe on the single-node Docker Compose
setup. For a sharded cluster, split the guard from the refund — atomic
compare-and-set on the reservation cell alone (`claim_compensation.lua`), refund
via the client (`rdb.Incr`, cluster routes it correctly), then finalize. A
`COMPENSATING` cell left by a crash between steps is healed by the reconciler.

**Dead-letter & reconciler.** Bind the consumer with `MaxDeliver(5)`; when the
worker still can't persist, the message lands on the `ORDERS_DLQ` subject. The
**reconciler worker** consumes it and calls the same compensation — closing the
loop from "payment accepted" back to "stock refunded":

```
orders.created ─► order-processor ─(fail ×5)─► ORDERS_DLQ ─► reconciler ─► release_stock.lua
                                                                    └──► metrics: compensation_total
```

Because delivery is at-least-once, every handler must be **idempotent** (unique
constraint on `(user_id, event_id)` is the final safety net for the rare
message that persists *and* compensates).

---

## Payment Window & Stock Rollback (15-Minute Timeout)

Once admitted, a buyer gets a **15-minute payment window**. If the order is not
paid by then, we must — reliably and exactly once — cancel the order in the
database **and** return the held stock to Redis (units flood back into the
sharded counters so newly-admitted waiters can buy them).

### Invariants (duplicate-safe design)

- Cancel + restock must happen **exactly once** even when: the timer fires and
  the JetStream consumer crashes mid-processing (redelivery), the reconciler
  also sees the same order, and the payment webhook arrives at the same moment.
- Payment and timeout are in a **race**; both sides perform an atomic
  compare-and-swap on the DB order status. Whichever wins decides the outcome.
- The Redis reservation must never be INCR'd twice.
- Trigger *reliability* matters more than trigger *speed*: a missed trigger that
  heals via the reconciler beats a fast trigger that double-returns stock.

### Candidate mechanisms compared

| Dimension | A. Redis Keyspace Notifications (TTL) | B. Delayed Message (NATS scheduler / RabbitMQ TTL+DLX) | C. Cron / DB polling |
|---|---|---|---|
| Delivery guarantee | **At-most-once** (Pub/Sub, fire-and-forget) | **At-least-once** (JetStream delivers until ACK) | N/A (scan) |
| Durability / replay | None — missed event is gone forever | Durable, replayable stream | DB is the source of truth |
| Firing precision | **Unbounded** — expiry is probabilistic (Redis samples ~20 keys ×10/s under load); events can be seconds/minutes late | Exact `@at` (NATS ≥2.12 scheduler) or queue-TTL buckets (Rabbit) | Seconds-to-minute granularity |
| Subscriber failure | **Loss** — subscriber down on expiry = no event at all | Survives; message redelivered after reconnect | No loss; next sweep finds it |
| Redis Cluster / managed | Node-local events; often no `CONFIG SET` on managed Redis | Stream replicated to replicas | N/A |
| Operational cost | +CPU on every write; opt-in `notify-keyspace-events` | One JetStream stream; reuse existing NATS | Index scan at poll frequency; needs leader lock |
| Best role | Best-effort signal only | ✅ **Primary timeout trigger** | ✅ **Safety-net reconciler** |

**Verdict.** None alone is enough:

- **Redis keyspace notifications are disqualified as authoritative.** `expired`
  events ride pub/sub: at-most-once. A consumer restart at the critical moment
  *permanently* loses the event → order stays pending forever, stock stays
  locked. TTL expiry also isn't guaranteed to fire at `expires_at` under load.
  Useful only as a cheap hint, never as the mechanism.
- **Delayed/Durable messaging is the primary trigger.** OpenWar already runs
  NATS; JetStream's native **message scheduler** (≥2.12, `allow_msg_schedules`)
  stores the timer durably and fires at an exact `@at`. RabbitMQ's TTL+DLX is
  the equivalent pattern (queue `order.payment.delay` with `x-message-ttl:`
  900000ms → DLX `order.payment.timeout`), with a caveat: a slow consumer
  head-of-lines every later timer, so bucket multiple TTL queues or use plugins.
- **Cron/DB polling is the reconciler.** As a *primary* it can't keep up at
  flash-sale scale (full-scan load, minute-level latency). As a periodic
  **safety net** (every 2 min, indexed cursor scan, leader-locked) it makes the
  whole design complete — the documented escape hatch for any message lost
  between Redis, DB, and NATS. Late cancellation (+2 min) is acceptable because
  restock is eventually consistent.

### Final topology (layered defense)

```
[order created]
   │
   ├─► DB insert: status=PENDING_PAYMENT, expires_at=now+15m   (source of truth)
   │
   ├─► JetStream  orders.created         → Order Worker → persist → ack
   │
   └─► JetStream scheduler (≥2.12): schedules.timeout.<orderID>
              Nats-Schedule @at now+15m → orders.timeout    (durable arm)

   +15m ─► orders.timeout fires (at-least-once)
                 │
                 ▼
        Timeout Consumer (worker)
          1. idem claim: Redis SETNX cancel:<orderID>          (dup guard)
          2. CAS: UPDATE orders SET status='CANCELLED_TIMEOUT'
                    WHERE order_id=?
                      AND status='PENDING_PAYMENT'
                      AND expires_at <= NOW()
                    ├─ 0 rows → payment won the race → ACK, no restock
                    └─ 1 row  → proceed
          3. restock: release_stock.lua (RESERVED→INCR→COMPENSATED)   (§above)
          4. publish orders.cancelled (outbox) → downstream (audit/coupon)
          5. ACK  (only after full success)

Payment path (the racer):
  payment webhook → UPDATE ... SET status='PAID'
                    WHERE order_id=? AND status='PENDING_PAYMENT'
                      AND expires_at > NOW()
  ├─ wins  → reservation → PUBLISHED → COMPLETED  (delete key; no need to
  │          cancel the timer — the timeout consumer will no-op on status)
  └─ loses → already CANCELLED_TIMEOUT → issue refund

Safety net: reconciler every 2 min (leader-locked, cursor-paginated)
   SELECT ... FROM orders
   WHERE status='PENDING_PAYMENT' AND expires_at <= NOW()
   → same CAS → same guarded compensation (overlap is safe by design)
```

### Duration strategy (who owns the 15 minutes?)

| Store | Value | Role |
|---|---|---|
| DB `orders.expires_at` | `now + PAYMENT_WINDOW` | **Source of truth** — queryable, indexable, race-safe |
| JetStream schedule | `@at now + PAYMENT_WINDOW` | **Trigger** — durable, precise, redelivers |
| Redis `reservation:<id>` TTL | `PAYMENT_WINDOW + grace` (25m) | **Leak failsafe only** — must be *strictly longer* than `expires_at` |

The Redis reservation key is *not* the timeout mechanism. If its TTL expired
*first* it would INCR a unit that a 14:59 payment legitimately sold. Every
actual compensation must pass the DB CAS first; Redis merely auto-cleans
crash-orphaned cells.

### Code — arm the timer (NATS ≥2.12 scheduler)

```go
js, _ := nc.JetStream()
_, err := js.AddStream(&nats.StreamConfig{
    Name: "ORDERS_TIMEOUT",
    Subjects: []string{"schedules.timeout.>", "orders.timeout"},
    Storage: nats.FileStorage,
    AllowMsgSchedules: true, // enable one-shot @at schedules
})
```

```go
func (s *Service) armTimeout(orderID string, deadline time.Time) error {
    js, _ := s.nc.JetStream()
    // ONE schedule per subject — the scheduler stores schedules as
    // roll-up subject messages, so a unique subject per order is mandatory.
    m := nats.NewMsg("schedules.timeout." + orderID)
    m.Data = []byte(orderID)
    m.Header.Set("Nats-Schedule", "@at "+deadline.UTC().Format(time.RFC3339))
    m.Header.Set("Nats-Schedule-Target", "orders.timeout")
    ack, err := js.PublishMsg(m)
    if err != nil || ack == nil {
        // Order is already in the DB; the reconciler will still cancel it.
        // Log + alert: schedule lost, safety net covers correctness.
        return err
    }
    return nil
}
```

```go
sub, _ := js.SubscribeSync("orders.timeout",
    nats.Durable("payment-timeout-worker"),
    nats.ManualAck(), nats.MaxDeliver(5), nats.AckWait(60*time.Second))

for {
    msg, err := sub.NextMsg(5 * time.Second)
    if err != nil { continue }
    if err := handleTimeout(string(msg.Data)); err != nil {
        msg.NakWithDelay(2 * time.Second) // JetStream redelivers (at-least-once)
        continue
    }
    msg.Ack() // ack only after CAS + restock fully succeeded
}
```

```go
func (s *Service) handleTimeout(orderID string) error {
    // 1. cheap duplicate guard before any DB write
    if !s.rdb.SetNX(ctx, "cancel:"+orderID, 1, 24*time.Hour).Val() {
        return errDuplicate // redelivery of an already-handled timeout
    }

    // 2. THE decision point — a single CAS. 0 rows ⇒ payment won the race.
    res, err := s.db.Exec(`
        UPDATE orders
        SET status='CANCELLED_TIMEOUT', cancelled_at=NOW(), version=version+1
        WHERE order_id = ? AND status='PENDING_PAYMENT' AND expires_at <= NOW()`,
        orderID)
    if err != nil { return err }
    if n, _ := res.RowsAffected(); n == 0 {
        return nil // paid or already cancelled → no restock, this is a no-op
    }

    // 3. restock exactly once via the reservation guard (release_stock.lua)
    if _, err := s.compensate(ctx, "reservation:"+orderID, resv.ShardKey, 1, "payment_timeout"); err != nil {
        return err // leave unacked; the SETNX + RESERVED guard forbid a double-INCR
    }

    // 4. outbox → orders.cancelled for audit, coupon, reward, etc.
    return s.publish(ctx, orderCancelled{OrderID: orderID, Reason: "TIMEOUT"})
}
```

### Code — safety-net reconciler

```go
// One leader (distributed lock). Every 2 minutes. Cursor-paginated scan
// over the (status, expires_at) index — no table scans at scale.
const page = 500
var lastID, total int64
for {
    rows, err := s.db.Query(`
        SELECT order_id, id FROM orders
        WHERE status='PENDING_PAYMENT' AND expires_at <= NOW() AND id > ?
        ORDER BY id LIMIT ?`, lastID, page)
    if err != nil { break }
    empty := true
    for rows.Next() {
        empty = false
        var id int64; var oid string
        rows.Scan(&id, &oid); lastID = id
        if err := s.handleTimeout(oid); err != nil { total++ } // retried next sweep
    }
    if empty { break }
}
```

The reconciler deliberately overlaps with the message path **by design**: both
funnel into the same CAS + guarded compensation, so overlap is safe and doubles
as the retry layer for the rare lost-schedule case. `handleTimeout` is cheap to
run idempotently.

### Why not cancel the JetStream schedule on payment?

"Cancel timer + check status" adds a second racy step for zero benefit. The
timeout consumer already short-circuits on `status != 'PENDING_PAYMENT'`. Let
the timer fire and be idempotently skipped — one code path to reason about.

### Metrics & alerts

- `payment_timeout_total{result=closed|skipped_paid|duplicate}`
- `payment_timeout_compensation_failed_total` — 🔴 alert: stock locked longer than grace
- `payment_timeout_reconciler_last_scan_ms`, `..._rows`
- 🔴 Alert: any `PENDING_PAYMENT` row with `expires_at` older than 30 min (reconciler stuck)

---

## Custom API Gateway & Middleware Chain

Built on the Go standard library `net/http` — zero web-framework dependency, full
control over the request lifecycle. Middleware composes as
`func(http.Handler) http.Handler` decorators.

```go
func Chain(ms ...func(http.Handler) http.Handler) func(http.Handler) http.Handler {
    return func(next http.Handler) http.Handler {
        for i := len(ms) - 1; i >= 0; i-- {
            next = ms[i](next)
        }
        return next
    }
}
```

Per-route pipeline for protected endpoints:

```text
recovery → requestID → logger → cors → jwtAuth
        → virtualWaitingRoom (admission-token check)
        → tokenBucket (Redis Lua)
        → idempotency (UUID v4 claim, per user+event)
        → reverseProxy (httputil.ReverseProxy)
```

```go
protected := Chain(
    middleware.Recovery(),
    middleware.RequestID(),
    middleware.Logger(slog.Default()),
    middleware.CORS(allowedOrigins),
    middleware.JWT(verifyKey),
    middleware.VirtualWaitingRoom(room, admitKey), // validate admission token
    middleware.TokenBucket(bucketClient),          // Redis Lua rate limit
    middleware.Idempotency(rdb, 24*time.Hour),     // double-submit guard
)

mux.Handle("/event/{id}/purchase", protected(proxy.Forward(backendURL)))
mux.HandleFunc("/event/{id}/queue/heartbeat", heartbeatHandler) // bypasses guard
```

The `httputil.ReverseProxy` adds connection pooling, chunked/Upgrade (WebSocket)
support and hop-by-hop header handling for free; a custom `Rewrite` hook injects
`X-Forwarded-*` headers, per-event bucket keys, and reads the idempotency head
out of the request context.

---

## Repository Layout

```
.
├── cmd/
│   ├── gateway/              # API Gateway entrypoint (net/http server)
│   ├── worker/               # Admission + order worker + reconciler (NATS)
│   └── backend-demo/         # Minimal demo backend (inventory + checkout)
├── internal/
│   ├── config/               # Env + YAML loader, validation
│   ├── router/               # net/http ServeMux wiring, per-route protected chain
│   ├── middleware/           # recovery, requestID, logger, cors, jwt,
│   │                         #   virtualwaitingroom, tokenbucket, idempotency
│   ├── ratelimiter/          # Redis Lua token bucket (script + client wrapper)
│   ├── inventory/            # sharded stock: Seeder + ShardRouter + compensation
│   ├── waitingroom/          # sorted-set queue, heartbeat, admission worker
│   ├── queue/                # NATS JetStream stream/consumer setup, publishers
│   ├── order/                # order writer, payment webhook, timeout consumer,
│   │                         #   reconciler + compensation triggers
│   ├── store/                # PostgreSQL (pgxpool): users, products, orders
│   │                         #   worker persists orders; seed command seeds
│   │                         #   product catalog + demo users
│   ├── proxy/                # httputil.ReverseProxy wrapper + forwarding headers
│   └── metrics/              # Prometheus counters/histograms (/metrics)
├── migrations/               # Versioned SQL migrations (embedded via go:embed)
├── internal/lua/             # Lua sources (canonical; embedded at build via go:embed)
│   ├── token_bucket.lua
│   ├── reserve_stock.lua
│   ├── claim_idem.lua
│   ├── admit_batch.lua
│   ├── consume_admit.lua
│   └── release_stock.lua
├── deploy/
│   ├── docker-compose.yml
│   └── Dockerfile
├── test/                     # k6 load-test scenarios
├── go.mod
├── Makefile
└── README.md
```

---

## Getting Started (Docker)

```bash
docker compose -f deploy/docker-compose.yml up --build
```

This brings up, wired together:

| Service | Image | Exposes |
|---|---|---|
| `gateway` | `deploy/Dockerfile` (`cmd/gateway`) | `:8080` |
| `worker` | same image, `cmd/worker` mode | — |
| `backend-demo` | same image, `cmd/backend-demo` mode | `:9001` (internal) |
| `redis` | `redis:7` | `:6379` |
| `nats` | `nats:2.10` (JetStream enabled) | `:4222` |
| `postgres` | `postgres:16` (openwar/openwar, DB `openwar`) | `:5432` |

The **worker** is the only Postgres client. It opens the store on boot, applies
the embedded migrations in `migrations/` (tracked via `openwar.schema_migrations`),
and persists each `orders.created` event into `openwar.orders` with
`status = 'PENDING_PAYMENT'` and `expires_at = now + PAYMENT_WINDOW` — the
source of truth the payment-timeout consumer and reconciler CAS against (v0.2).

The schema contains three tables:

| Table | Purpose |
|---|---|
| `users` | Registered buyers (seeded by `seed-events`; FK target for orders) |
| `products` | Catalog — one row per sellable SKU (seeded by `seed-events`) |
| `orders` | Durable orders — PK `order_id`, FK to `users` + `products` |

Run `seed-events` before any purchase to ensure the FK targets exist in
Postgres (the demo JWT claims `uid=user-42`).

Seed demo data and partitioned inventory, then simulate a war:

```bash
docker compose exec gateway /app/openwar seed-events \
    --event flash-sale-001 --qty 1000 --shards 32
docker compose run --rm k6 run /scripts/flashsale.js
```

---

## Configuration

Environment variables honored by the gateway and worker:

| Variable | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `:8080` | Gateway listen address |
| `REDIS_ADDR` | `localhost:6379` | Redis (bucket + queue + inventory state) |
| `NATS_URL` | `nats://localhost:4222` | NATS connection |
| `DATABASE_URL` | `postgres://openwar:openwar@localhost:5432/openwar` | PostgreSQL DSN (**worker** — durable order store / source of truth) |
| `BACKEND_URL` | `http://backend-demo:9001` | Upstream for proxied routes |
| `ADMISSION_RATE` | `100` | Users admitted per second (per event) |
| `ADMISSION_TTL` | `5m` | Admission-token lifetime |
| `HEARTBEAT_INTERVAL` | `15s` | Client ping cadence (page JS) |
| `HEARTBEAT_TTL` | `45s` | Liveness key TTL (3× interval) |
| `IDEMPOTENCY_WINDOW` | `24h` | Idempotency-key retention / replay window |
| `INVENTORY_SHARDS` | `32` | Partitions per SKU counter |
| `PAYMENT_WINDOW` | `15m` | Time allowed to pay before auto-cancel (order `expires_at`) |
| `RESERVATION_TTL` | `25m` | Redis reservation leak failsafe (window + grace; **must** exceed `PAYMENT_WINDOW`) |

Per-event tuning lives in the event config (seed time):

| Field | Meaning |
|---|---|
| `capacity` | Token bucket burst — max simultaneous buyers |
| `refill_rate` | Sustained bookings/sec the backend can survive |
| `inventory` | Total stock pre-warmed across `INVENTORY_SHARDS` counters |
| `admission_rate` | Live users admitted per second (tie to healthy-worker count) |
| `payment_window` | Per-event override of `PAYMENT_WINDOW` |

---

## API Reference

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/event/{id}/queue` | user JWT | Join the waiting room → `201` `{sessionID, position}` |
| `GET` | `/event/{id}/queue/status` | session cookie | `{position, estimatedWait, admitted}`; SSE upgrade supported |
| `POST` | `/event/{id}/queue/heartbeat` | session cookie | Refresh liveness (client pings every 15s) → `{position, admitted}` |
| `POST` | `/event/{id}/admit` | admission token | Exchange token for checkout capability (single use) |
| `POST` | `/event/{id}/purchase` | admission token + `Idempotency-Key` | **Rate-limited.** Sharded DECR + publish `orders.created`; compensation on failure |
| `GET` | `/metrics` | — | Prometheus scrape target |
| `GET` | `/healthz` | — | Liveness probe |

`Idempotency-Key` is **required** on `POST /event/{id}/purchase` and must be a
**UUID v4** generated by the client once per checkout attempt.

Rate-limit responses include standard headers so clients can back off:

```text
HTTP/1.1 429 Too Many Requests
Retry-After: 3
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 0
```

Waiting-room / heartbeat responses:

```json
{ "roomId": "flash-sale-001", "decision": "wait",
  "position": 4521, "estimatedWaitMinutes": 6,
  "admitted": false, "numberOfWaitingUsers": 22908 }
```

Idempotency responses:

```text
HTTP/1.1 409 Conflict
X-Idempotent-Replay: true        <- replayed an already-executed request
{ "error": "duplicate request", "idempotencyKey": "3f8a…" }

HTTP/1.1 422 Unprocessable Entity <- same key, different payload
{ "error": "Idempotency-Key reused with a different payload" }
```

---

## Load Testing & Monitoring

The `test/` folder ships a k6 scenario that simulates 10K concurrent users
joining, polling, purchasing — with a `1%` client that re-sends an
`Idempotency-Key` to exercise the replay path. Monitor on the Prometheus
`/metrics` endpoint:

- **QPS / p99 latency** per route and per middleware stage
- **Rate-limit violations** (429 count by key)
- **Queue depth** (ZCARD gauge per event) — drives admission-rate autoscaling
- **Heartbeat evictions** (`ZREM` zombies per second) and session TTL expiry
- **Idempotency replays** (409 vs 422 counters, cache-hit ratio)
- **Compensations** (`compensation_total{reason}`) — alert on sustained > 0
- **Redeliveries & DLQ depth** (NATS JetStream `nats_server_consumer_*`)

Expected shape under a healthy load test:

| Metric | Target |
|---|---|
| Queue join (ZADD) | sub-ms, 500K/s capacity on one Redis node |
| Token check (Lua) | < 2 ms |
| Sharded inventory DECR | ~1 ms, zero oversell, hot-key spread across shards |
| Heartbeat + position (pipeline) | < 2 ms |
| Idempotency claim (Lua) | < 1 ms |
| Order persistence | 1K/s steady, decoupled by NATS |
| Compensation on injectable NATS fault | < 500 ms to restore stock |

---

## Roadmap

| Milestone | Scope |
|---|---|
| **v0.1 — Core MVP** | Gateway; token bucket + sharded inventory (Lua); waiting room + heartbeat; idempotency keys; demo backend; Docker Compose |
| **v0.2 — Event-driven & compensation** | NATS JetStream streams/consumers; reservation state machine; DLQ → reconciler → stock restore; JetStream scheduler payment timeout (15 min) → CAS cancel + restock |
| **v0.3 — Real-time queue** | SSE/WebSocket position updates via `queue.notify`, adaptive admission rate |
| **v0.4 — Anti-abuse** | Bot detection (device fingerprint, per-IP bucket), randomized pre-sale window, circuit breakers |

---

## License

MIT. Built for the war rooms of high-concurrency commerce.