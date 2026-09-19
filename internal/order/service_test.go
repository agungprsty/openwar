package order_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/nats-io/nats.go"
	"github.com/openwar/openwar/internal/inventory"
	"github.com/openwar/openwar/internal/order"
	"github.com/redis/go-redis/v9"
)

type mockJS struct {
	nats.JetStreamContext
	published  int
	publishErr error
}

func (m *mockJS) Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	if m.publishErr != nil {
		return nil, m.publishErr
	}
	m.published++
	return &nats.PubAck{Stream: "test", Sequence: 1}, nil
}

func (m *mockJS) PublishMsg(msg *nats.Msg, opts ...nats.PubOpt) (*nats.PubAck, error) {
	if m.publishErr != nil {
		return nil, m.publishErr
	}
	m.published++
	return &nats.PubAck{Stream: "test", Sequence: 1}, nil
}

func TestService_Place_Success(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer s.Close()

	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	ctx := context.Background()

	rdb.Set(ctx, "inventory:TICKET1:shard:0", 10, 0)
	router := inventory.NewShardRouter(rdb, 1)

	js := &mockJS{}
	svc := order.NewService(rdb, nil, js, router, 5*time.Minute)

	req := order.Order{
		UserID:  "user1",
		EventID: "evt1",
		Sku:     "TICKET1",
		Qty:     1,
	}

	orderID, err := svc.Place(ctx, req)
	if err != nil {
		t.Fatalf("expected place to succeed, got: %v", err)
	}
	if orderID == "" {
		t.Fatalf("expected non-empty orderID")
	}

	stock, _ := rdb.Get(ctx, "inventory:TICKET1:shard:0").Int()
	if stock != 9 {
		t.Errorf("expected 9 stock left, got %v", stock)
	}

	if js.published != 1 {
		t.Errorf("expected 1 NATS publish, got %d", js.published)
	}

	res := rdb.Get(ctx, "reservation:"+orderID).Val()
	if res != "RESERVED" {
		t.Errorf("expected reservation state RESERVED, got %v", res)
	}
}

func TestService_Place_SoldOut(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer s.Close()

	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	ctx := context.Background()

	router := inventory.NewShardRouter(rdb, 1)
	js := &mockJS{}
	svc := order.NewService(rdb, nil, js, router, 5*time.Minute)

	req := order.Order{
		UserID:  "user2",
		EventID: "evt1",
		Sku:     "TICKET2",
		Qty:     1,
	}

	_, err = svc.Place(ctx, req)
	if !errors.Is(err, order.ErrSoldOut) {
		t.Fatalf("expected ErrSoldOut, got: %v", err)
	}
}

func TestService_Place_NatsFailureCompensates(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer s.Close()

	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	ctx := context.Background()

	rdb.Set(ctx, "inventory:TICKET3:shard:0", 10, 0)
	router := inventory.NewShardRouter(rdb, 1)

	js := &mockJS{publishErr: errors.New("nats down")}
	svc := order.NewService(rdb, nil, js, router, 5*time.Minute)

	req := order.Order{
		UserID:  "user3",
		EventID: "evt1",
		Sku:     "TICKET3",
		Qty:     1,
	}

	_, err = svc.Place(ctx, req)
	if err == nil {
		t.Fatalf("expected error due to nats failure, got nil")
	}

	// Verify stock was refunded back to 10
	stock, _ := rdb.Get(ctx, "inventory:TICKET3:shard:0").Int()
	if stock != 10 {
		t.Errorf("expected stock to be refunded to 10, got %v", stock)
	}
}

func TestService_ArmTimeout(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer s.Close()

	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	ctx := context.Background()

	router := inventory.NewShardRouter(rdb, 1)
	js := &mockJS{}
	svc := order.NewService(rdb, nil, js, router, 5*time.Minute)

	err = svc.ArmTimeout(ctx, "ord-123", time.Now().Add(5*time.Minute))
	if err != nil {
		t.Fatalf("expected ArmTimeout to succeed, got: %v", err)
	}

	if js.published != 1 {
		t.Errorf("expected 1 NATS publish, got %d", js.published)
	}
}
