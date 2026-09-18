package queue

import (
	"github.com/nats-io/nats.go"
)

const (
	OrdersCreated          = "orders.created"
	OrdersCancelled        = "orders.cancelled"
	OrdersTimeout          = "orders.timeout"
	OrdersDLQ              = "orders.dlq"
	QueueNotify            = "queue.notify"
	SchedulesTimeoutPrefix = "schedules.timeout."
)

// Setup ensures required JetStream streams exist (idempotent on boot).
func Setup(nc *nats.Conn) (nats.JetStreamContext, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	defs := []*nats.StreamConfig{
		{
			Name:     "ORDERS",
			Subjects: []string{OrdersCreated, OrdersCancelled},
			Storage:  nats.FileStorage,
			MaxAge:   24 * 3600 * 1e9, // 24h in ns
		},
		{
			Name:        "ORDERS_TIMEOUT",
			Subjects:    []string{SchedulesTimeoutPrefix + ">", OrdersTimeout},
			Storage:     nats.FileStorage,
			MaxAge:      24 * 3600 * 1e9,
			AllowMsgTTL: true, // JetStream header-initiated message scheduling / TTL
		},
		{
			Name:     "ORDERS_DLQ",
			Subjects: []string{OrdersDLQ},
			Storage:  nats.FileStorage,
			MaxAge:   7 * 24 * 3600 * 1e9, // 7 days in ns
		},
		{
			Name:     "QUEUE_NOTIFY",
			Subjects: []string{QueueNotify},
			Storage:  nats.FileStorage,
			MaxAge:   3600 * 1e9, // 1h in ns
		},
	}

	for _, d := range defs {
		if _, err := js.AddStream(d); err != nil && !isAlreadyExists(err) {
			return nil, err
		}
	}

	return js, nil
}

func isAlreadyExists(err error) bool {
	return err != nil && (err.Error() == "stream name already in use" ||
		err.Error() == "stream already exists")
}
