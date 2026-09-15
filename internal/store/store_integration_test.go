//go:build integration

package store

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(context.Background(), "postgres://openwar:openwar@localhost:5432/openwar?sslmode=disable")
	if err != nil {
		t.Skipf("postgres not available: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.pool.Exec(ctx, `TRUNCATE openwar.orders, openwar.products, openwar.users CASCADE`)
		s.Close()
	})
	return s
}

func seedUserAndProduct(t *testing.T, s *Store, userID, sku string) {
	t.Helper()
	ctx := context.Background()
	if err := s.UpsertUser(ctx, userID, userID+"@demo.openwar", "Demo"); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if err := s.UpsertProduct(ctx, Product{
		Sku:             sku,
		Name:            "Demo",
		PriceCents:      5000,
		InventoryTotal:  100,
		InventoryShards: 4,
		Status:          "ACTIVE",
	}); err != nil {
		t.Fatalf("seed product: %v", err)
	}
}

// ---------------------------------------------------------------------------
// users
// ---------------------------------------------------------------------------

func TestUpsertUser(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if err := s.UpsertUser(ctx, "u1", "a@b.com", "Alice"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertUser(ctx, "u1", "a2@b.com", "Alice 2"); err != nil {
		t.Fatal(err) // idempotent upsert must not fail
	}

	n, err := s.CountUsers(ctx)
	if err != nil || n != 1 {
		t.Fatalf("count = %d err=%v, want 1", n, err)
	}
}

// ---------------------------------------------------------------------------
// products
// ---------------------------------------------------------------------------

func TestUpsertProduct(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if err := s.UpsertProduct(ctx, Product{Sku: "sku-1", Name: "Widget", InventoryTotal: 100, InventoryShards: 4, Status: "ACTIVE"}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertProduct(ctx, Product{Sku: "sku-1", Name: "Widget 2", InventoryTotal: 200, InventoryShards: 8, Status: "ACTIVE"}); err != nil {
		t.Fatal(err) // idempotent upsert
	}

	p, err := s.GetProduct(ctx, "sku-1")
	if err != nil {
		t.Fatal(err)
	}
	if p.Name != "Widget 2" || p.InventoryTotal != 200 || p.InventoryShards != 8 {
		t.Fatalf("upsert did not update: %+v", p)
	}
}

func TestGetProductNotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.GetProduct(context.Background(), "nope")
	if err != pgx.ErrNoRows {
		t.Fatalf("want pgx.ErrNoRows, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// orders
// ---------------------------------------------------------------------------

func TestInsertOrderAndIdempotentRedelivery(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedUserAndProduct(t, s, "user-x", "sku-a")

	now := time.Now().UTC()
	o := Order{
		OrderID: "ord-test-001",
		UserID:  "user-x",
		EventID: "e1",
		Sku:     "sku-a",
		Qty:     1,
		Status:  "PENDING_PAYMENT",
		Expires: now.Add(15 * time.Minute),
	}

	if err := s.InsertOrder(ctx, o); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if err := s.InsertOrder(ctx, o); err != ErrRowExists {
		t.Fatalf("redelivery: want ErrRowExists, got %v", err)
	}

	got, err := s.GetOrder(ctx, o.OrderID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != "PENDING_PAYMENT" {
		t.Fatalf("status = %q, want PENDING_PAYMENT", got.Status)
	}
	if got.Qty != 1 || got.Sku != "sku-a" {
		t.Fatalf("row mismatch: %+v", got)
	}
	if !got.Expires.After(now) || got.Expires.After(now.Add(20*time.Minute)) {
		t.Fatalf("expires = %v, want ~now+15m", got.Expires)
	}

	n, err := s.CountOrders(ctx)
	if err != nil || n != 1 {
		t.Fatalf("count = %d err=%v, want 1 row after redelivery", n, err)
	}
}

func TestUserEventSafetyNet(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedUserAndProduct(t, s, "u1", "sk")

	if err := s.InsertOrder(ctx, Order{
		OrderID: "ord-net-1", UserID: "u1", EventID: "evt",
		Sku: "sk", Qty: 1, Expires: time.Now().Add(15 * time.Minute),
	}); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if err := s.InsertOrder(ctx, Order{
		OrderID: "ord-net-2", UserID: "u1", EventID: "evt",
		Sku: "sk", Qty: 1, Expires: time.Now().Add(15 * time.Minute),
	}); err == nil {
		t.Fatal("want unique(user_id, event_id) violation, got nil")
	}
}

func TestInsertOrderFailsWithoutProductOrUser(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	err := s.InsertOrder(ctx, Order{
		OrderID: "ord-fk-1", UserID: "ghost", EventID: "e",
		Sku: "ghost-sku", Qty: 1, Expires: time.Now().Add(time.Minute),
	})
	if err == nil {
		t.Fatal("want FK violation for unknown user+product, got nil")
	}
}
