package order

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/openwar/openwar/internal/inventory"
	"github.com/openwar/openwar/internal/metrics"
	"github.com/redis/go-redis/v9"
)

type DLQProcessor struct {
	rdb    *redis.Client
	router *inventory.ShardRouter
	logger *slog.Logger
}

func NewDLQProcessor(rdb *redis.Client, router *inventory.ShardRouter, logger *slog.Logger) *DLQProcessor {
	return &DLQProcessor{
		rdb:    rdb,
		router: router,
		logger: logger,
	}
}

// HandleDLQMsg processes messages delivered to the DLQ when worker attempts expire.
func (p *DLQProcessor) HandleDLQMsg(ctx context.Context, msg *nats.Msg) error {
	var ev Event
	if err := json.Unmarshal(msg.Data, &ev); err != nil {
		p.logger.Error("dlq unmarshal failed", "err", err)
		return err
	}

	resKey := "reservation:" + ev.OrderID
	shardKey := ev.ShardKey
	if shardKey == "" {
		home := shardForUser(ev.UserID, p.router.Shards())
		shardKey = "inventory:" + ev.Sku + ":shard:" + string(rune('0'+home))
	}

	if _, err := p.router.Compensate(ctx, resKey, shardKey, ev.Qty, "dlq_exhausted"); err != nil {
		p.logger.Error("dlq compensation failed", "orderId", ev.OrderID, "err", err)
		return err
	}

	metrics.CompensationTotal.WithLabelValues("dlq_exhausted").Inc()
	p.logger.Info("dlq message compensated and stock restored", "orderId", ev.OrderID, "sku", ev.Sku)
	return nil
}
