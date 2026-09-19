package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/openwar/openwar/migrations"
)

// Store is the PostgreSQL access layer (worker + seed commands).
type Store struct {
	pool *pgxpool.Pool
}

// Open connects, then ensures the schema exists (idempotent CREATE IF NOT EXISTS).
func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.EnsureSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) EnsureSchema(ctx context.Context) error {
	return migrations.Migrate(ctx, s.pool)
}

// ---------------------------------------------------------------------------
// users
// ---------------------------------------------------------------------------

// UpsertUser inserts or refreshes a user row. Used by the seed command.
func (s *Store) UpsertUser(ctx context.Context, id, email, fullName string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO openwar.users (id, email, full_name)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, full_name = EXCLUDED.full_name`,
		id, email, fullName)
	if err != nil {
		return fmt.Errorf("upsert user %s: %w", id, err)
	}
	return nil
}

func (s *Store) CountUsers(ctx context.Context) (int64, error) {
	var n int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM openwar.users`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// ---------------------------------------------------------------------------
// products
// ---------------------------------------------------------------------------

type Product struct {
	Sku             string
	Name            string
	PriceCents      int64
	InventoryTotal  int64
	InventoryShards int
	Status          string
	Created         time.Time
}

// UpsertProduct inserts or refreshes a product row. Used by the seed command.
func (s *Store) UpsertProduct(ctx context.Context, p Product) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO openwar.products
			(sku, name, price_cents, inventory_total, inventory_shards, status)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (sku) DO UPDATE SET
			name            = EXCLUDED.name,
			price_cents     = EXCLUDED.price_cents,
			inventory_total = EXCLUDED.inventory_total,
			inventory_shards = EXCLUDED.inventory_shards,
			status          = EXCLUDED.status`,
		p.Sku, p.Name, p.PriceCents, p.InventoryTotal, p.InventoryShards, p.Status)
	if err != nil {
		return fmt.Errorf("upsert product %s: %w", p.Sku, err)
	}
	return nil
}

func (s *Store) GetProduct(ctx context.Context, sku string) (*Product, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT sku, name, price_cents, inventory_total, inventory_shards, status, created_at
		FROM openwar.products WHERE sku = $1`, sku)
	var p Product
	if err := row.Scan(&p.Sku, &p.Name, &p.PriceCents, &p.InventoryTotal,
		&p.InventoryShards, &p.Status, &p.Created); err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *Store) CountProducts(ctx context.Context) (int64, error) {
	var n int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM openwar.products`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// ---------------------------------------------------------------------------
// orders
// ---------------------------------------------------------------------------

// Order mirrors the openwar.orders row. Worker-only writer today.
type Order struct {
	OrderID string
	UserID  string
	EventID string
	Sku     string
	Qty     int64
	Status  string
	Expires time.Time
	Created time.Time
	Version int64
}

// ErrRowExists is returned when the insert collides with an existing order
// (redelivery of an at-least-once message) or the (user_id, event_id) net.
var ErrRowExists = errors.New("order already persisted")

// InsertOrder persists an order idempotently for at-least-once delivery.
// A redelivered message returns ErrRowExists instead of duplicating the row.
// FK constraints on user_id and sku are enforced by Postgres; the seed
// command must have created the referenced rows before any purchase can land.
func (s *Store) InsertOrder(ctx context.Context, o Order) error {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO openwar.orders
			(order_id, user_id, event_id, sku, qty, status, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (order_id) DO NOTHING`,
		o.OrderID, o.UserID, o.EventID, o.Sku, o.Qty, defaultStatus(o.Status), o.Expires)
	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrRowExists
	}
	return nil
}

// GetOrder returns a single order by id.
func (s *Store) GetOrder(ctx context.Context, orderID string) (*Order, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT order_id, user_id, event_id, sku, qty, status, expires_at, created_at, version
		FROM openwar.orders WHERE order_id = $1`, orderID)
	var o Order
	if err := row.Scan(&o.OrderID, &o.UserID, &o.EventID, &o.Sku,
		&o.Qty, &o.Status, &o.Expires, &o.Created, &o.Version); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, pgx.ErrNoRows
		}
		return nil, err
	}
	return &o, nil
}

func (s *Store) CountOrders(ctx context.Context) (int64, error) {
	var n int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM openwar.orders`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// CancelOrderTimeout performs an atomic CAS status update for an expired order.
// Returns true if the order status was updated from PENDING_PAYMENT to CANCELLED_TIMEOUT.
func (s *Store) CancelOrderTimeout(ctx context.Context, orderID string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE openwar.orders
		SET status = 'CANCELLED_TIMEOUT', cancelled_at = NOW(), version = version + 1
		WHERE order_id = $1 AND status = 'PENDING_PAYMENT' AND expires_at <= NOW()`, orderID)
	if err != nil {
		return false, fmt.Errorf("cancel order timeout %s: %w", orderID, err)
	}
	return tag.RowsAffected() > 0, nil
}

// FetchExpiredOrders retrieves expired PENDING_PAYMENT orders using cursor pagination.
func (s *Store) FetchExpiredOrders(ctx context.Context, limit int) ([]Order, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT order_id, user_id, event_id, sku, qty, status, expires_at, created_at, version
		FROM openwar.orders
		WHERE status = 'PENDING_PAYMENT' AND expires_at <= NOW()
		ORDER BY created_at ASC
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch expired orders: %w", err)
	}
	defer rows.Close()

	var out []Order
	for rows.Next() {
		var o Order
		if err := rows.Scan(&o.OrderID, &o.UserID, &o.EventID, &o.Sku,
			&o.Qty, &o.Status, &o.Expires, &o.Created, &o.Version); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func defaultStatus(s string) string {
	if s == "" {
		return "PENDING_PAYMENT"
	}
	return s
}
