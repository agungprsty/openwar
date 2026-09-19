package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/openwar/openwar/internal/config"
	"github.com/openwar/openwar/internal/inventory"
	"github.com/openwar/openwar/internal/order"
	"github.com/openwar/openwar/internal/queue"
	"github.com/openwar/openwar/internal/store"
	"github.com/openwar/openwar/internal/waitingroom"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		logger.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, PoolSize: 128})
	nc, err := nats.Connect(cfg.NATSURL)
	if err != nil {
		logger.Error("nats unavailable", "err", err)
		os.Exit(1)
	}
	defer nc.Close()

	js, err := queue.Setup(nc)
	if err != nil {
		logger.Error("jetstream setup failed", "err", err)
		os.Exit(1)
	}

	orders, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("postgres unavailable", "err", err)
		os.Exit(1)
	}
	defer orders.Close()
	logger.Info("postgres connected", "url", config.SanitizeDSN(cfg.DatabaseURL))

	router := inventory.NewShardRouter(rdb, cfg.InventoryShards)
	orderSvc := order.NewService(rdb, nc, js, router, cfg.ReservationTTL)
	timeoutProc := order.NewTimeoutProcessor(rdb, js, orders, router, logger)
	dlqProc := order.NewDLQProcessor(rdb, router, logger)

	// Reconciler safety net worker
	reconciler := order.NewReconcilerWorker(rdb, orders, timeoutProc, 2*time.Minute, logger)
	reconciler.Start()
	defer reconciler.Stop()

	// Admission worker
	event := getenv("ADMISSION_EVENT", "flash-sale-001")
	room := waitingroom.New(rdb, cfg.HeartbeatTTL, cfg.AdmissionTTL)
	tick := time.Second

	worker := waitingroom.NewWorker(room, event, cfg.AdmissionRate, tick, func(_ context.Context, event, sid string) {
		payload, _ := json.Marshal(map[string]string{"event": event, "session": sid, "admitted": "1"})
		if _, perr := js.Publish(queue.QueueNotify, payload); perr != nil {
			logger.Warn("queue.notify publish failed", "session", sid, "err", perr)
		}
	})
	worker.Start()
	defer worker.Stop()
	logger.Info("admission worker started", "event", event, "rate", cfg.AdmissionRate)

	var wg sync.WaitGroup

	// 1. Order Worker (orders.created)
	orderSub, err := js.SubscribeSync(queue.OrdersCreated,
		nats.Durable("order-processor"),
		nats.ManualAck(),
		nats.AckWait(30*time.Second),
		nats.MaxDeliver(5),
	)
	if err != nil {
		logger.Error("subscribe orders.created failed", "err", err)
		os.Exit(1)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			msg, err := orderSub.NextMsg(2 * time.Second)
			if err != nil {
				continue
			}
			if err := handleOrder(ctx, rdb, orders, orderSvc, cfg.PaymentWindow, logger, msg); err != nil {
				logger.Error("order message failed", "err", err)
				msg.NakWithDelay(2 * time.Second)
				continue
			}
			msg.Ack()
		}
	}()

	// 2. Timeout Worker (orders.timeout)
	timeoutSub, err := js.SubscribeSync(queue.OrdersTimeout,
		nats.Durable("payment-timeout-processor"),
		nats.ManualAck(),
		nats.AckWait(30*time.Second),
		nats.MaxDeliver(5),
	)
	if err != nil {
		logger.Error("subscribe orders.timeout failed", "err", err)
		os.Exit(1)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			msg, err := timeoutSub.NextMsg(2 * time.Second)
			if err != nil {
				continue
			}
			orderID := string(msg.Data)
			if err := timeoutProc.ProcessTimeout(ctx, orderID); err != nil && !errors.Is(err, order.ErrDuplicateTimeout) {
				logger.Error("timeout message failed", "orderId", orderID, "err", err)
				msg.NakWithDelay(2 * time.Second)
				continue
			}
			msg.Ack()
		}
	}()

	// 3. DLQ Worker (orders.dlq)
	dlqSub, err := js.SubscribeSync(queue.OrdersDLQ,
		nats.Durable("order-dlq-processor"),
		nats.ManualAck(),
		nats.AckWait(30*time.Second),
	)
	if err == nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				msg, err := dlqSub.NextMsg(2 * time.Second)
				if err != nil {
					continue
				}
				_ = dlqProc.HandleDLQMsg(ctx, msg)
				msg.Ack()
			}
		}()
	}

	logger.Info("all workers started")
	<-ctx.Done()
	logger.Info("worker shutting down, draining subscribers...")

	// Drain NATS subscriptions gracefully
	_ = orderSub.Drain()
	_ = timeoutSub.Drain()
	if dlqSub != nil {
		_ = dlqSub.Drain()
	}

	shutdownDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		logger.Info("all in-flight messages processed cleanly")
	case <-time.After(15 * time.Second):
		logger.Warn("worker shutdown timed out waiting for in-flight messages")
	}
}

func handleOrder(ctx context.Context, rdb *redis.Client, orders *store.Store, orderSvc *order.Service, paymentWindow time.Duration, logger *slog.Logger, msg *nats.Msg) error {
	var ev order.Event
	if err := json.Unmarshal(msg.Data, &ev); err != nil {
		return err
	}

	expiresAt := time.Now().Add(paymentWindow)
	err := orders.InsertOrder(ctx, store.Order{
		OrderID: ev.OrderID,
		UserID:  ev.UserID,
		EventID: ev.EventID,
		Sku:     ev.Sku,
		Qty:     ev.Qty,
		Status:  "PENDING_PAYMENT",
		Expires: expiresAt,
	})
	if err != nil && err != store.ErrRowExists {
		return err
	}

	// Flip RESERVED → PUBLISHED so compensation is disarmed.
	if err := rdb.Set(ctx, "reservation:"+ev.OrderID, "PUBLISHED", 0).Err(); err != nil {
		return err
	}

	// Arm scheduled NATS timeout message
	if err := orderSvc.ArmTimeout(ctx, ev.OrderID, expiresAt); err != nil {
		logger.Warn("arm timeout failed (reconciler safety net active)", "orderId", ev.OrderID, "err", err)
	}

	logger.Info("order persisted & timeout armed", "orderId", ev.OrderID, "sku", ev.Sku,
		"redelivery", err == store.ErrRowExists)
	return nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
