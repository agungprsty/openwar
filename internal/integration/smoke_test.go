//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/openwar/openwar/internal/inventory"
	"github.com/openwar/openwar/internal/ratelimiter"
	"github.com/openwar/openwar/internal/waitingroom"
	"github.com/redis/go-redis/v9"
)

func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 9})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis not available: %v", err)
	}
	rdb.FlushDB(context.Background())
	t.Cleanup(func() { rdb.FlushDB(context.Background()) })
	return rdb
}

func TestShardedInventoryReserveAndCompensate(t *testing.T) {
	rdb := newTestRedis(t)
	ctx := context.Background()

	seeder := inventory.NewSeeder(rdb, 4)
	if err := seeder.SeedPartitionedStock(ctx, "sku-1", 10); err != nil {
		t.Fatalf("seed: %v", err)
	}
	avail, err := seeder.Available(ctx, "sku-1")
	if err != nil || avail != 10 {
		t.Fatalf("available = %d, err=%v; want 10", avail, err)
	}

	router := inventory.NewShardRouter(rdb, 4)

	// Reserve 10 units across users; 11th must fail.
	keys := make(map[string]bool)
	for i := 0; i < 10; i++ {
		key, err := router.Reserve(ctx, "sku-1", "user-"+string(rune('a'+i)))
		if err != nil {
			t.Fatalf("reserve %d: %v", i, err)
		}
		keys[key] = true
	}
	if len(keys) != 4 {
		t.Logf("warn: expected all 4 shards used after 10 reserves, got %d", len(keys))
	}
	if _, err := router.Reserve(ctx, "sku-1", "user-last"); err != inventory.ErrSoldOut {
		t.Fatalf("want ErrSoldOut, got %v", err)
	}

	// Compensate a previously reserved key with a fake reservation cell.
	resKey := "reservation:order-1"
	seeder.SeedPartitionedStock(ctx, "sku-2", 5) // fresh sku for compensate flow
	shardKey, err := router.Reserve(ctx, "sku-2", "u1")
	if err != nil {
		t.Fatalf("reserve sku-2: %v", err)
	}
	rdb.Set(ctx, resKey, "RESERVED", 0)
	before := availOf(t, rdb, shardKey)
	if _, err := router.Compensate(ctx, resKey, shardKey, 1, "test"); err != nil {
		t.Fatalf("compensate: %v", err)
	}
	if got := availOf(t, rdb, shardKey); got != before+1 {
		t.Fatalf("after compensate want %d back, got %d", before+1, got)
	}

	// Second compensation must be an idempotent no-op (no double INCR).
	if _, err := router.Compensate(ctx, resKey, shardKey, 1, "test"); err != nil {
		t.Fatalf("re-compensate: %v", err)
	}
	if got := availOf(t, rdb, shardKey); got != before+1 {
		t.Fatalf("double compensation bumped stock: got %d want %d", got, before+1)
	}
}

func TestTokenBucket(t *testing.T) {
	rdb := newTestRedis(t)
	ctx := context.Background()

	lim := ratelimiter.New(rdb, 3, 1, ratelimiter.FailClosed)
	key := "{event:e1}:bucket:user1"

	// burst capacity = 3
	if r := lim.Allow(ctx, key); !r.Allowed {
		t.Fatal("req1 should pass")
	}
	if r := lim.Allow(ctx, key); !r.Allowed {
		t.Fatal("req2 should pass")
	}
	if !lim.Allow(ctx, key).Allowed {
		t.Fatal("req3 should pass")
	}
	if lim.Allow(ctx, key).Allowed {
		t.Fatal("req4 over capacity should be denied")
	}
}

func TestWaitingRoomJoinHeartbeatAdmit(t *testing.T) {
	rdb := newTestRedis(t)
	ctx := context.Background()

	room := waitingroom.New(rdb, 5*time.Second, 10*time.Second)

	pos, err := room.Join(ctx, "e1", "s1")
	if err != nil || pos != 0 {
		t.Fatalf("join s1 pos=%d err=%v want 0", pos, err)
	}
	pos, _ = room.Join(ctx, "e1", "s2")
	if pos != 1 {
		t.Fatalf("join s2 pos=%d want 1", pos)
	}

	// Heartbeat refreshes liveness + returns position atomically.
	p, adm, err := room.Heartbeat(ctx, "e1", "s1")
	if err != nil || p != 0 || adm {
		t.Fatalf("hb s1 pos=%d adm=%v err=%v", p, adm, err)
	}

	// Admit a live session and consume the token once.
	// (Direct Admit requires a live heartbeat; s2 has none yet.)
	room.Heartbeat(ctx, "e1", "s2")
	ok, err := room.Admit(ctx, "e1", "s2")
	if err != nil || !ok {
		t.Fatalf("admit s2 ok=%v err=%v", ok, err)
	}
	has, _ := room.HasAdmission(ctx, "e1", "s2")
	if !has {
		t.Fatal("admission token should exist")
	}
	used, _ := room.ConsumeAdmission(ctx, "e1", "s2")
	if !used {
		t.Fatal("consume should succeed once")
	}
	if has2, _ := room.HasAdmission(ctx, "e1", "s2"); has2 {
		t.Fatal("token must be single-use")
	}
}

func TestWaitingRoomWorkerBatchAdmit(t *testing.T) {
	rdb := newTestRedis(t)
	ctx := context.Background()

	room := waitingroom.New(rdb, 5*time.Second, 10*time.Second)
	admitted := make(chan string, 10)
	worker := waitingroom.NewWorker(room, "e2", 10, 100*time.Millisecond,
		func(_ context.Context, _ string, sid string) { admitted <- sid })
	worker.Start()
	defer worker.Stop()

	// 3 live sessions + 1 zombie (no heartbeat).
	room.Join(ctx, "e2", "s1")
	room.Join(ctx, "e2", "s2")
	room.Join(ctx, "e2", "s3")
	room.Join(ctx, "e2", "s4") // zombie: joins but never heartbeats
	room.Heartbeat(ctx, "e2", "s1")
	room.Heartbeat(ctx, "e2", "s2")
	room.Heartbeat(ctx, "e2", "s3")

	got := map[string]bool{}
	deadline := time.After(3 * time.Second)
	for len(got) < 3 {
		select {
		case sid := <-admitted:
			got[sid] = true
		case <-deadline:
			t.Fatalf("timeout waiting for admissions, got %v", got)
		}
	}

	// Zombie s4 must NOT be admitted.
	if got["s4"] {
		t.Fatal("zombie session was admitted")
	}
	for _, s := range []string{"s1", "s2", "s3"} {
		if !got[s] {
			t.Fatalf("live session %s was not admitted: %v", s, got)
		}
	}

	// Queue must now be drained (ZPOPMIN removed all members).
	if depth := rdb.ZCard(ctx, "room:e2:queue").Val(); depth != 0 {
		t.Fatalf("queue depth = %d, want 0", depth)
	}
}

func availOf(t *testing.T, rdb *redis.Client, key string) int64 {
	t.Helper()
	v, err := rdb.Get(context.Background(), key).Int64()
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return v
}
