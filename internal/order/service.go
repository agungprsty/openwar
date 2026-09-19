package order

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/openwar/openwar/internal/inventory"
	"github.com/openwar/openwar/internal/middleware"
	"github.com/openwar/openwar/internal/queue"
	"github.com/openwar/openwar/internal/resilience"
	"github.com/redis/go-redis/v9"
)

var ErrSoldOut = errors.New("sold out")

// Order is the place-order request from the backend to the service.
type Order struct {
	OrderID string
	UserID  string
	EventID string
	Sku     string
	Qty     int64
	Created time.Time
}

// Event is the durable order event published to NATS (orders.created) and
// consumed by the order worker. One definition, shared by publisher and
// consumer, so the JSON contract cannot drift.
type Event struct {
	OrderID  string `json:"orderId"`
	UserID   string `json:"userId"`
	EventID  string `json:"eventId"`
	Sku      string `json:"sku"`
	Qty      int64  `json:"qty"`
	ShardKey string `json:"shardKey"`
	Created  int64  `json:"createdAt"`
}

type Service struct {
	rdb            *redis.Client
	nc             *nats.Conn
	js             nats.JetStreamContext
	router         *inventory.ShardRouter
	reservationTTL time.Duration
	natsCB         *resilience.CircuitBreaker
}

func NewService(rdb *redis.Client, nc *nats.Conn, js nats.JetStreamContext, router *inventory.ShardRouter, reservationTTL time.Duration) *Service {
	return &Service{
		rdb:            rdb,
		nc:             nc,
		js:             js,
		router:         router,
		reservationTTL: reservationTTL,
		natsCB:         resilience.NewCircuitBreaker(resilience.DefaultConfig()),
	}
}

// Place reserves a shard, marks a reservation, and publishes the durable event.
// On publish failure the stock is restored exactly once via the reservation
// state machine.
func (s *Service) Place(ctx context.Context, o Order) (string, error) {
	o.OrderID = genOrderID(ctx)
	if o.Qty <= 0 {
		o.Qty = 1
	}

	// 1. reserve on a shard; returns the winning key for exact refund.
	invKey, err := s.router.Reserve(ctx, o.Sku, o.UserID)
	if err != nil {
		return "", ErrSoldOut
	}

	// 2. mark reservation RESERVED (TTL auto-expires abandoned holds).
	resKey := "reservation:" + o.OrderID
	if err := s.rdb.Set(ctx, resKey, "RESERVED", s.reservationTTL).Err(); err != nil {
		s.router.Compensate(ctx, resKey, invKey, o.Qty, "redis_down_after_reserve")
		return "", err
	}

	// 3. publish durable event (nil PubAck ⇒ no stream stored it ⇒ failure).
	payload, _ := json.Marshal(Event{
		OrderID:  o.OrderID,
		UserID:   o.UserID,
		EventID:  o.EventID,
		Sku:      o.Sku,
		Qty:      o.Qty,
		ShardKey: invKey,
		Created:  time.Now().UnixMilli(),
	})
	if err := s.publish(ctx, payload); err != nil {
		// 4. COMPENSATE — the Lua guard prevents a double-INCR.
		s.router.Compensate(ctx, resKey, invKey, o.Qty, "nats_publish_failed")
		return "", fmt.Errorf("publish: %w", err)
	}

	return o.OrderID, nil
}

func (s *Service) publish(ctx context.Context, payload []byte) error {
	return s.natsCB.Execute(ctx, func() error {
		var err error
		var ack *nats.PubAck
		backoff := 100 * time.Millisecond
		
		for i := 0; i < 3; i++ {
			ack, err = s.js.Publish(queue.OrdersCreated, payload)
			if err == nil && ack != nil {
				return nil
			}
			if i == 2 {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff *= 2
			}
		}
		
		if err == nil {
			err = errors.New("no ack")
		}
		return fmt.Errorf("nats publish failed after retries: %w", err)
	})
}

// ArmTimeout schedules a one-shot payment timeout event using JetStream's native scheduler.
func (s *Service) ArmTimeout(ctx context.Context, orderID string, deadline time.Time) error {
	return s.natsCB.Execute(ctx, func() error {
		m := nats.NewMsg(queue.SchedulesTimeoutPrefix + orderID)
		m.Data = []byte(orderID)
		m.Header.Set("Nats-Schedule", "@at "+deadline.UTC().Format(time.RFC3339))
		m.Header.Set("Nats-Schedule-Target", queue.OrdersTimeout)

		var err error
		var ack *nats.PubAck
		backoff := 100 * time.Millisecond
		
		for i := 0; i < 3; i++ {
			ack, err = s.js.PublishMsg(m)
			if err == nil && ack != nil {
				return nil
			}
			if i == 2 {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff *= 2
			}
		}
		
		if err == nil {
			err = errors.New("no ack")
		}
		return fmt.Errorf("arm timeout publish failed after retries: %w", err)
	})
}

// genOrderID prefixes the order id with the request id when present (typed
// context key) so support can correlate an order to its request.
func genOrderID(ctx context.Context) string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("ord-%d", time.Now().UnixNano())
	}
	if rid := middleware.RequestIDFrom(ctx); rid != "" {
		return "ord-" + rid + "-" + hex.EncodeToString(b[:4])
	}
	return "ord-" + hex.EncodeToString(b)
}
