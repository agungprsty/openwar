-- Initial schema for OpenWar
CREATE SCHEMA IF NOT EXISTS openwar;

CREATE TABLE IF NOT EXISTS openwar.users (
    id         TEXT        PRIMARY KEY,
    email      TEXT        NOT NULL UNIQUE,
    full_name  TEXT        NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS openwar.products (
    sku              TEXT        PRIMARY KEY,
    name             TEXT        NOT NULL,
    price_cents      BIGINT      NOT NULL DEFAULT 0,
    inventory_total  BIGINT      NOT NULL DEFAULT 0,
    inventory_shards INT         NOT NULL DEFAULT 32,
    status           TEXT        NOT NULL DEFAULT 'ACTIVE',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS openwar.orders (
    order_id     TEXT        PRIMARY KEY,
    user_id      TEXT        NOT NULL REFERENCES openwar.users(id),
    event_id     TEXT        NOT NULL,
    sku          TEXT        NOT NULL REFERENCES openwar.products(sku),
    qty          BIGINT      NOT NULL DEFAULT 1,
    status       TEXT        NOT NULL DEFAULT 'PENDING_PAYMENT',
    expires_at   TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    cancelled_at TIMESTAMPTZ,
    version      BIGINT      NOT NULL DEFAULT 1,
    CONSTRAINT orders_user_event_once UNIQUE (user_id, event_id)
);

CREATE INDEX IF NOT EXISTS orders_status_expires_idx
    ON openwar.orders (status, expires_at);
