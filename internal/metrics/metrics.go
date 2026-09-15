package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	RequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "openwar_requests_total",
		Help: "Total requests handled by the gateway.",
	}, []string{"method", "route", "status"})

	RequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "openwar_request_duration_seconds",
		Help:    "Request latency by route.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route"})

	// Rate limiter metrics are intentionally scoped by event only (bounded
	// cardinality). Per-user bucket keys would explode label count with the
	// user base.
	RateLimiterErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "openwar_ratelimiter_errors_total",
		Help: "Token bucket Redis errors by event.",
	}, []string{"event"})

	RateLimitedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "openwar_rate_limited_total",
		Help: "Requests denied by the token bucket by event.",
	}, []string{"event"})

	TokensRemaining = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "openwar_tokens_remaining",
		Help: "Current token bucket tokens by event.",
	}, []string{"event"})

	IdempotencyReplays = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "openwar_idempotency_replays_total",
		Help: "Replayed idempotency requests (confirm vs conflict).",
	}, []string{"result"})

	QueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "openwar_queue_depth",
		Help: "Waiting room queue depth per event.",
	}, []string{"event"})

	AdmittedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "openwar_admitted_total",
		Help: "Sessions admitted per event.",
	}, []string{"event"})

	CompensationTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "openwar_compensation_total",
		Help: "Stock compensation events by reason.",
	}, []string{"reason"})

	InventoryReserved = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "openwar_inventory_reserved_total",
		Help: "Inventory units reserved per sku.",
	}, []string{"sku"})

	InventorySoldOut = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "openwar_inventory_soldout_total",
		Help: "Sold-out rejections per sku.",
	}, []string{"sku"})
)

func init() {
	prometheus.MustRegister(
		RequestsTotal,
		RequestDuration,
		RateLimiterErrors,
		RateLimitedTotal,
		TokensRemaining,
		IdempotencyReplays,
		QueueDepth,
		AdmittedTotal,
		CompensationTotal,
		InventoryReserved,
		InventorySoldOut,
	)
}
