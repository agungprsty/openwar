package ratelimiter_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/openwar/openwar/internal/ratelimiter"
	"github.com/redis/go-redis/v9"
)

func setupTestLimiter(t *testing.T, cap, refill float64, fs ratelimiter.FailStrategy) (*ratelimiter.Limiter, *miniredis.Miniredis, *redis.Client) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	lim := ratelimiter.New(rdb, cap, refill, fs)
	return lim, s, rdb
}

func TestLimiter_AllowWithinCapacity(t *testing.T) {
	ctx := context.Background()
	lim, _, _ := setupTestLimiter(t, 5, 1, ratelimiter.FailOpen)

	if lim.Capacity() != 5 {
		t.Errorf("expected capacity 5, got %d", lim.Capacity())
	}

	// Consume 5 tokens
	for i := 0; i < 5; i++ {
		res := lim.Allow(ctx, "test-bucket")
		if res.Err != nil {
			t.Fatalf("unexpected error on token %d: %v", i, res.Err)
		}
		if !res.Allowed {
			t.Errorf("expected token %d to be allowed", i)
		}
	}

	// 6th token should be rejected
	res := lim.Allow(ctx, "test-bucket")
	if res.Allowed {
		t.Errorf("expected 6th request to be rejected, but was allowed")
	}
	if res.RetryAfter <= 0 {
		t.Errorf("expected positive RetryAfter, got %v", res.RetryAfter)
	}
}

func TestLimiter_Refill(t *testing.T) {
	ctx := context.Background()
	lim, _, _ := setupTestLimiter(t, 2, 10, ratelimiter.FailOpen) // 10 tokens per second refill

	// Exhaust bucket
	lim.Allow(ctx, "refill-bucket")
	lim.Allow(ctx, "refill-bucket")

	res := lim.Allow(ctx, "refill-bucket")
	if res.Allowed {
		t.Fatalf("expected bucket to be empty")
	}

	// Wait 250ms -> should refill at least 2 tokens
	time.Sleep(250 * time.Millisecond)

	res = lim.Allow(ctx, "refill-bucket")
	if !res.Allowed {
		t.Errorf("expected request to be allowed after refill interval")
	}
}

func TestLimiter_FailOpenWhenRedisFails(t *testing.T) {
	ctx := context.Background()
	lim, s, _ := setupTestLimiter(t, 10, 1, ratelimiter.FailOpen)

	// Close Redis
	s.Close()

	res := lim.Allow(ctx, "broken-bucket")
	if res.Err == nil {
		t.Error("expected error when Redis is down")
	}
	if !res.Allowed {
		t.Error("expected fail-open limiter to allow request despite Redis error")
	}
}

func TestLimiter_FailClosedWhenRedisFails(t *testing.T) {
	ctx := context.Background()
	lim, s, _ := setupTestLimiter(t, 10, 1, ratelimiter.FailClosed)

	// Close Redis
	s.Close()

	res := lim.Allow(ctx, "broken-bucket")
	if res.Err == nil {
		t.Error("expected error when Redis is down")
	}
	if res.Allowed {
		t.Error("expected fail-closed limiter to deny request when Redis error occurs")
	}
}
