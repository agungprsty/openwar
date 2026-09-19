package inventory_test

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/openwar/openwar/internal/inventory"
	"github.com/redis/go-redis/v9"
)

func TestRouter_Reserve_Success(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer s.Close()

	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	ctx := context.Background()

	// Seed inventory: 10 stock in shard 0, shard 1 is empty
	rdb.Set(ctx, "inventory:TICKET1:shard:0", 10, 0)
	router := inventory.NewShardRouter(rdb, 2)

	invKey, err := router.Reserve(ctx, "TICKET1", "user1")
	if err != nil {
		t.Fatalf("expected reserve to succeed, got: %v", err)
	}
	if invKey == "" {
		t.Fatalf("expected non-empty invKey")
	}

	// Verify remaining stock in that shard is 9
	stock, err := rdb.Get(ctx, invKey).Int()
	if err != nil || stock != 9 {
		t.Errorf("expected 9 stock left, got %v (err: %v)", stock, err)
	}
}

func TestRouter_Reserve_SoldOut(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer s.Close()

	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	ctx := context.Background()
	router := inventory.NewShardRouter(rdb, 2)

	// No stock seeded -> immediate sold out latch should be flipped after sweep
	_, err = router.Reserve(ctx, "TICKET2", "user2")
	if err != inventory.ErrSoldOut {
		t.Fatalf("expected ErrSoldOut, got: %v", err)
	}

	// Verify soldout latch is set
	if exists := rdb.Exists(ctx, "soldout:TICKET2").Val(); exists != 1 {
		t.Errorf("expected soldout latch to be set")
	}

	// Next call should fail fast
	_, err = router.Reserve(ctx, "TICKET2", "user3")
	if err != inventory.ErrSoldOut {
		t.Fatalf("expected ErrSoldOut on second call, got: %v", err)
	}
}

func TestRouter_Compensate(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer s.Close()

	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	ctx := context.Background()

	// Seed 9 stock, and a reservation of 1
	rdb.Set(ctx, "inventory:TICKET3:shard:0", 9, 0)
	rdb.Set(ctx, "reservation:res1", "RESERVED", 0)

	router := inventory.NewShardRouter(rdb, 1)

	remaining, err := router.Compensate(ctx, "reservation:res1", "inventory:TICKET3:shard:0", 1, "test")
	if err != nil {
		t.Fatalf("expected compensate to succeed, got: %v", err)
	}
	if remaining != 10 {
		t.Errorf("expected 10 remaining stock, got %v", remaining)
	}

	// Verify state changed
	state := rdb.Get(ctx, "reservation:res1").Val()
	if state != "COMPENSATED" {
		t.Errorf("expected state to be COMPENSATED, got %s", state)
	}

	// Double compensate should fail to increment since reservation state changed
	remaining2, err := router.Compensate(ctx, "reservation:res1", "inventory:TICKET3:shard:0", 1, "test")
	if err != nil {
		t.Fatalf("expected 2nd compensate to succeed without error, got: %v", err)
	}
	// The lua script returns remaining = 0 when already handled.
	if remaining2 != 0 {
		t.Errorf("expected returned remaining to be 0 for already handled, got %v", remaining2)
	}
}
