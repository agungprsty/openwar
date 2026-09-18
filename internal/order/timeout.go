package order

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/openwar/openwar/internal/inventory"
	"github.com/openwar/openwar/internal/queue"
	"github.com/openwar/openwar/internal/store"
	"github.com/redis/go-redis/v9"
)

var ErrDuplicateTimeout = errors.New("timeout already processed")

type TimeoutProcessor struct {
	rdb    *redis.Client
	js     nats.JetStreamContext
	orders *store.Store
	router *inventory.ShardRouter
	logger *slog.Logger
}

func NewTimeoutProcessor(rdb *redis.Client, js nats.JetStreamContext, orders *store.Store, router *inventory.ShardRouter, logger *slog.Logger) *TimeoutProcessor {
	return &TimeoutProcessor{
		rdb:    rdb,
		js:     js,
		orders: orders,
		router: router,
		logger: logger,
	}
}

// ProcessTimeout handles order payment timeout.
// 1. SETNX duplicate guard in Redis ("cancel:"+orderID)
// 2. Fetch order to get user_id & sku
// 3. SQL CAS: UPDATE openwar.orders SET status='CANCELLED_TIMEOUT'... WHERE status='PENDING_PAYMENT' AND expires_at <= NOW()
// 4. If updated, call router.Compensate to restore stock to Redis.
// 5. Publish orders.cancelled event.
func (p *TimeoutProcessor) ProcessTimeout(ctx context.Context, orderID string) error {
	guardKey := "cancel:" + orderID
	ok, err := p.rdb.SetNX(ctx, guardKey, "1", 24*time.Hour).Result()
	if err != nil {
		return fmt.Errorf("timeout guard redis: %w", err)
	}
	if !ok {
		return ErrDuplicateTimeout
	}

	ord, err := p.orders.GetOrder(ctx, orderID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // order not found, nothing to timeout
		}
		return err
	}

	cancelled, err := p.orders.CancelOrderTimeout(ctx, orderID)
	if err != nil {
		return err
	}
	if !cancelled {
		return nil // paid or already cancelled, no-op
	}

	resKey := "reservation:" + orderID
	home := shardForUser(ord.UserID, p.router.Shards())
	invKey := fmt.Sprintf("inventory:%s:shard:%d", ord.Sku, home)

	if _, cErr := p.router.Compensate(ctx, resKey, invKey, ord.Qty, "payment_timeout"); cErr != nil {
		p.logger.Error("stock compensation failed during timeout", "orderId", orderID, "err", cErr)
	}

	payload, _ := json.Marshal(map[string]string{
		"orderId": orderID,
		"reason":  "TIMEOUT",
		"sku":     ord.Sku,
	})
	_, _ = p.js.Publish(queue.OrdersCancelled, payload)

	p.logger.Info("order timed out and stock restored", "orderId", orderID, "sku", ord.Sku)
	return nil
}

func shardForUser(userID string, shards int) int {
	if shards <= 0 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(userID))
	return int(h.Sum32() % uint32(shards))
}
