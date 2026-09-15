package inventory

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type Seeder struct {
	rdb    *redis.Client
	shards int
}

func NewSeeder(rdb *redis.Client, shards int) *Seeder {
	return &Seeder{rdb: rdb, shards: shards}
}

// SeedPartitionedStock splits stock across N shard counters so that
// sum(shards) == stock, spreading hot write keys across cluster slots.
func (s *Seeder) SeedPartitionedStock(ctx context.Context, sku string, stock int64) error {
	if s.shards < 1 {
		return fmt.Errorf("inventory: shards must be >= 1")
	}
	base := stock / int64(s.shards)
	rem := stock % int64(s.shards)
	pipe := s.rdb.TxPipeline()
	for i := 0; i < s.shards; i++ {
		qty := base
		if int64(i) < rem {
			qty++
		}
		pipe.Set(ctx, fmt.Sprintf("inventory:%s:shard:%d", sku, i), qty, 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	return s.rdb.Del(ctx, "soldout:"+sku).Err() // clear sellout latch on re-run
}

// Available returns the total remaining across all shards (for dashboards/tests).
func (s *Seeder) Available(ctx context.Context, sku string) (int64, error) {
	keys := make([]string, 0, s.shards)
	for i := 0; i < s.shards; i++ {
		keys = append(keys, fmt.Sprintf("inventory:%s:shard:%d", sku, i))
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return 0, err
	}
	var total int64
	for _, v := range vals {
		if v == nil {
			continue
		}
		var n int64
		fmt.Sscanf(v.(string), "%d", &n)
		total += n
	}
	return total, nil
}
