package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/openwar/openwar/internal/config"
	"github.com/openwar/openwar/internal/order"
	"github.com/openwar/openwar/internal/queue"
	"github.com/openwar/openwar/internal/store"
	"github.com/openwar/openwar/internal/waitingroom"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()

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

	// Durable order store: source of truth for the payment race (v0.2).
	orders, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("postgres unavailable", "err", err)
		os.Exit(1)
	}
	defer orders.Close()
	logger.Info("postgres connected", "url", cfg.DatabaseURL)

	// Admission worker per configured event. In Docker the VIP event is
	// flash-sale-001; extend with a list of events when running many sales.
	event := getenv("ADMISSION_EVENT", "flash-sale-001")
	room := waitingroom.New(rdb, cfg.HeartbeatTTL, cfg.AdmissionTTL)
	tick := time.Second // batch = ADMISSION_RATE users/sec

	worker := waitingroom.NewWorker(room, event, cfg.AdmissionRate, tick, func(_ context.Context, event, sid string) {
		// Best-effort notify; pollers learn via heartbeat return instead.
		payload, _ := json.Marshal(map[string]string{"event": event, "session": sid, "admitted": "1"})
		if _, perr := js.Publish(queue.QueueNotify, payload); perr != nil {
			logger.Warn("queue.notify publish failed", "session", sid, "err", perr)
		}
	})
	worker.Start()
	logger.Info("admission worker started", "event", event, "rate", cfg.AdmissionRate)

	// Order worker (durable consumer, at-least-once, max 5 redeliveries).
	sub, err := js.SubscribeSync(queue.OrdersCreated,
		nats.Durable("order-processor"),
		nats.ManualAck(),
		nats.AckWait(30*time.Second),
		nats.MaxDeliver(5),
	)
	if err != nil {
		logger.Error("subscribe orders.created failed", "err", err)
		os.Exit(1)
	}
	logger.Info("order worker subscribed", "subject", queue.OrdersCreated)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			msg, err := sub.NextMsg(2 * time.Second)
			if err != nil {
				continue
			}
			if err := handleOrder(ctx, rdb, orders, cfg.PaymentWindow, logger, msg); err != nil {
				logger.Error("order message failed", "err", err)
				msg.NakWithDelay(2 * time.Second)
				continue
			}
			msg.Ack() // ack only after successful handling
		}
	}()

	<-ctx.Done()
	logger.Info("worker shutting down")
	worker.Stop()
}

// handleOrder persists the order to PostgreSQL idempotently (at-least-once),
// then disarms compensation by flipping RESERVED → PUBLISHED. The message is
// acked only after both succeed; redeliveries hit the (order_id) conflict net.
func handleOrder(ctx context.Context, rdb *redis.Client, orders *store.Store, paymentWindow time.Duration, logger *slog.Logger, msg *nats.Msg) error {
	var ev order.Event
	if err := json.Unmarshal(msg.Data, &ev); err != nil {
		return err
	}

	err := orders.InsertOrder(ctx, store.Order{
		OrderID: ev.OrderID,
		UserID:  ev.UserID,
		EventID: ev.EventID,
		Sku:     ev.Sku,
		Qty:     ev.Qty,
		Status:  "PENDING_PAYMENT",
		Expires: time.Now().Add(paymentWindow),
	})
	if err != nil && err != store.ErrRowExists {
		return err
	}

	// Flip RESERVED → PUBLISHED so compensation is disarmed.
	if err := rdb.Set(ctx, "reservation:"+ev.OrderID, "PUBLISHED", 0).Err(); err != nil {
		return err
	}
	logger.Info("order persisted", "orderId", ev.OrderID, "sku", ev.Sku,
		"redelivery", err == store.ErrRowExists)
	return nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
