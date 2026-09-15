package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/openwar/openwar/internal/config"
	"github.com/openwar/openwar/internal/inventory"
	"github.com/openwar/openwar/internal/store"
	"github.com/redis/go-redis/v9"
)

// runSeed pre-warms sharded inventory for demo events.
func runSeed(logger *slog.Logger) {
	fs := flag.NewFlagSet("seed-events", flag.ExitOnError)
	event := fs.String("event", "flash-sale-001", "event id / sku name")
	qty := fs.Int64("qty", 1000, "total stock")
	shards := fs.Int("shards", 0, "shard count (overrides INVENTORY_SHARDS)")
	fs.Parse(os.Args[2:])

	cfg := config.Load()
	if *shards > 0 {
		cfg.InventoryShards = *shards
	}

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, PoolSize: 64})

	seeder := inventory.NewSeeder(rdb, cfg.InventoryShards)
	sku := *event + ":ticket"
	if err := seeder.SeedPartitionedStock(ctx, sku, *qty); err != nil {
		logger.Error("seed failed", "err", err)
		return
	}

	// Clear any straggler queue so the next sale starts clean.
	rdb.Del(ctx, "room:"+*event+":queue")

	avail, _ := seeder.Available(ctx, sku)
	logger.Info("inventory seeded",
		"sku", sku,
		"event", *event,
		"qty", *qty,
		"shards", cfg.InventoryShards,
		"available", avail,
	)

	if err := seedCatalog(ctx, cfg.DatabaseURL, *event, sku, *qty, cfg.InventoryShards, logger); err != nil {
		logger.Error("catalog seed failed", "err", err)
	}
}

// seedCatalog upserts the product row and demo users into Postgres so order
// inserts satisfy the FK constraints. Redis is the hot path; Postgres is the
// durable catalog + order source of truth.
func seedCatalog(ctx context.Context, databaseURL, event, sku string, qty int64, shards int, logger *slog.Logger) error {
	orders, err := store.Open(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer orders.Close()

	if err := orders.UpsertProduct(ctx, store.Product{
		Sku:             sku,
		Name:            event + " ticket",
		PriceCents:      59000,
		InventoryTotal:  qty,
		InventoryShards: shards,
		Status:          "ACTIVE",
	}); err != nil {
		return err
	}

	// Demo users the JWT demo can act as (uid=user-42 is the live demo token).
	for i := 1; i <= 50; i++ {
		id := fmt.Sprintf("user-%d", i)
		if err := orders.UpsertUser(ctx, id, id+"@demo.openwar", "Demo User "+id); err != nil {
			return err
		}
	}

	logger.Info("catalog seeded", "event", event, "product", sku, "demoUsers", 50)
	return nil
}
