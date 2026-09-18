package inventory

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/openwar/openwar/internal/lua"
	"github.com/openwar/openwar/internal/metrics"
	"github.com/redis/go-redis/v9"
)

var ErrSoldOut = errors.New("sold out")

type ShardRouter struct {
	rdb     *redis.Client
	shards  int
	reserve *redis.Script
	release *redis.Script
}

func NewShardRouter(rdb *redis.Client, shards int) *ShardRouter {
	return &ShardRouter{
		rdb:     rdb,
		shards:  shards,
		reserve: redis.NewScript(lua.ReserveStock),
		release: redis.NewScript(lua.ReleaseStock),
	}
}

func (r *ShardRouter) Shards() int { return r.shards }

// shardFor deterministically assigns users to a home shard: same user → same
// shard, so contention is spread and their retries hit the same counter.
func shardFor(userID string, shards int) int {
	h := fnv.New32a()
	h.Write([]byte(userID))
	return int(h.Sum32() % uint32(shards))
}

// Reserve decrements one live shard, probing siblings on exhaustion. Returns
// the exact key that was reserved so compensation refunds the right counter.
func (r *ShardRouter) Reserve(ctx context.Context, sku, userID string) (string, error) {
	home := shardFor(userID, r.shards)

	if r.isSoldOut(ctx, sku) {
		metrics.InventorySoldOut.WithLabelValues(sku).Inc()
		return "", ErrSoldOut // fast-path latch
	}

	for offset := 0; offset < r.shards; offset++ { // at most one full sweep
		key := fmt.Sprintf("inventory:%s:shard:%d", sku, (home+offset)%r.shards)
		res, err := r.reserve.Run(ctx, r.rdb, []string{key}).Int64Slice()
		if err != nil {
			continue // Redis hiccup → try another shard
		}
		if res[0] == 1 {
			metrics.InventoryReserved.WithLabelValues(sku).Inc()
			return key, nil
		}
	}

	r.latchSoldOut(ctx, sku) // every shard empty → latch gate
	metrics.InventorySoldOut.WithLabelValues(sku).Inc()
	return "", ErrSoldOut
}

// Compensate restores stock guarded by the reservation state machine so two
// retriers can never double-INCR. Returns remaining-after-restore on success.
func (r *ShardRouter) Compensate(ctx context.Context, resKey, invKey string, qty int64, reason string) (int64, error) {
	res, err := r.release.Run(ctx, r.rdb, []string{resKey}, invKey, qty, reason).Result()
	if err != nil {
		return 0, err
	}
	arr, ok := res.([]interface{})
	if !ok || len(arr) < 2 {
		return 0, fmt.Errorf("release_stock: unexpected result %v", res)
	}
	restored := toInt(arr[0]) == 1
	remaining, _ := arr[1].(int64)
	if restored {
		metrics.CompensationTotal.WithLabelValues(reason).Inc()
		if sku := parseSKUFromInvKey(invKey); sku != "" {
			r.rdb.Del(ctx, "soldout:"+sku)
		}
	}
	return remaining, nil
}

func parseSKUFromInvKey(invKey string) string {
	s := strings.TrimPrefix(invKey, "inventory:")
	idx := strings.LastIndex(s, ":shard:")
	if idx <= 0 {
		return ""
	}
	return s[:idx]
}

func toInt(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	case string:
		var i int64
		fmt.Sscanf(n, "%d", &i)
		return i
	}
	return 0
}

func (r *ShardRouter) latchSoldOut(ctx context.Context, sku string) {
	r.rdb.Set(ctx, "soldout:"+sku, 1, 0)
}

func (r *ShardRouter) isSoldOut(ctx context.Context, sku string) bool {
	return r.rdb.Exists(ctx, "soldout:"+sku).Val() == 1
}
