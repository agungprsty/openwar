CREATE SCHEMA IF NOT EXISTS openwar;

-- Registered buyers. The demo JWT carries uid=user-42; seed-events upserts a
-- handful of demo users so orders satisfy the FK.
CREATE TABLE IF NOT EXISTS openwar.users (
    id         TEXT        PRIMARY KEY,
    email      TEXT        NOT NULL UNIQUE,
    full_name  TEXT        NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Catalog: one row per sellable SKU, pre-warmed by seed-events together with
-- the Redis sharded counters. inventory_total tracks all-time allocation;
-- the live sellable balance lives in Redis (hot path), not here.
CREATE TABLE IF NOT EXISTS openwar.products (
    sku              TEXT        PRIMARY KEY,
    name             TEXT        NOT NULL,
    price_cents      BIGINT      NOT NULL DEFAULT 0,
    inventory_total  BIGINT      NOT NULL DEFAULT 0,
    inventory_shards INT         NOT NULL DEFAULT 32,
    status           TEXT        NOT NULL DEFAULT 'ACTIVE',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Durable orders: source of truth for the payment race (v0.2).
-- Order Worker inserts here with status PENDING_PAYMENT and expires_at;
-- future timeout consumer / reconciler CAS on (status, version) from here.
CREATE TABLE IF NOT EXISTS openwar.orders (
    order_id    TEXT        PRIMARY KEY,
    user_id     TEXT        NOT NULL REFERENCES openwar.users(id),
    event_id    TEXT        NOT NULL,
    sku         TEXT        NOT NULL REFERENCES openwar.products(sku),
    qty         BIGINT      NOT NULL DEFAULT 1,
    status      TEXT        NOT NULL DEFAULT 'PENDING_PAYMENT',
    expires_at  TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    cancelled_at TIMESTAMPTZ,
    version     BIGINT      NOT NULL DEFAULT 1,

    -- Final safety net for the rare at-least-once message that survives
    -- both the worker and the reconciler compensation path.
    CONSTRAINT orders_user_event_once UNIQUE (user_id, event_id)
);

-- Driving index for the timeout scan: rows with status = PENDING_PAYMENT
-- and expires_at <= now() (reconciler cursor poll, v0.2).
CREATE INDEX IF NOT EXISTS orders_status_expires_idx
    ON openwar.orders (status, expires_at);