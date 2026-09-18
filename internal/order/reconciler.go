package order

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/openwar/openwar/internal/store"
	"github.com/redis/go-redis/v9"
)

type ReconcilerWorker struct {
	rdb       *redis.Client
	orders    *store.Store
	processor *TimeoutProcessor
	logger    *slog.Logger
	interval  time.Duration
	stop      chan struct{}
}

func NewReconcilerWorker(rdb *redis.Client, orders *store.Store, processor *TimeoutProcessor, interval time.Duration, logger *slog.Logger) *ReconcilerWorker {
	if interval <= 0 {
		interval = 2 * time.Minute
	}
	return &ReconcilerWorker{
		rdb:       rdb,
		orders:    orders,
		processor: processor,
		interval:  interval,
		logger:    logger,
		stop:      make(chan struct{}),
	}
}

func (w *ReconcilerWorker) Start() {
	go w.loop()
}

func (w *ReconcilerWorker) Stop() {
	close(w.stop)
}

func (w *ReconcilerWorker) loop() {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.tickOnce(context.Background())
		case <-w.stop:
			return
		}
	}
}

func (w *ReconcilerWorker) tickOnce(ctx context.Context) {
	// Leader election via Redis key lock so only one worker scans DB
	lockKey := "reconciler:leader"
	ok, err := w.rdb.SetNX(ctx, lockKey, "1", w.interval/2).Result()
	if err != nil || !ok {
		return // not leader or Redis error
	}

	expired, err := w.orders.FetchExpiredOrders(ctx, 100)
	if err != nil {
		w.logger.Error("reconciler fetch failed", "err", err)
		return
	}

	if len(expired) == 0 {
		return
	}

	w.logger.Info("reconciler scanning expired orders", "count", len(expired))
	reconciled := 0
	for _, ord := range expired {
		if err := w.processor.ProcessTimeout(ctx, ord.OrderID); err != nil {
			if !errors.Is(err, ErrDuplicateTimeout) {
				w.logger.Warn("reconciler process timeout error", "orderId", ord.OrderID, "err", err)
			}
			continue
		}
		reconciled++
	}
	if reconciled > 0 {
		w.logger.Info("reconciler finished scan", "reconciled", reconciled)
	}
}
