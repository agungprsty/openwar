package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/openwar/openwar/internal/config"
	"github.com/openwar/openwar/internal/inventory"
	"github.com/openwar/openwar/internal/order"
	"github.com/openwar/openwar/internal/queue"
	"github.com/redis/go-redis/v9"
)

const maxBodyBytes = 4 << 10

// PlaceRequest carries only what a buyer may decide; the identity comes from
// the gateway-authenticated X-User-ID header (set from the verified JWT).
type PlaceRequest struct {
	Sku string `json:"sku"`
	Qty int64  `json:"qty"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, PoolSize: 256})
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

	router := inventory.NewShardRouter(rdb, cfg.InventoryShards)
	svc := order.NewService(rdb, nc, js, router, cfg.ReservationTTL)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /checkout", func(w http.ResponseWriter, r *http.Request) {
		user := r.Header.Get("X-User-ID")
		if user == "" {
			// Direct exposure (outside the gateway) must not place orders.
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing X-User-ID"})
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		var req PlaceRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request"})
			return
		}
		if req.Sku == "" {
			req.Sku = "flash-sale-001:ticket"
		}

		oid, err := svc.Place(r.Context(), order.Order{
			UserID:  user,
			EventID: "flash-sale-001",
			Sku:     req.Sku,
			Qty:     req.Qty,
		})
		if err != nil {
			code := http.StatusInternalServerError
			msg := err.Error()
			if err == order.ErrSoldOut {
				code = http.StatusGone
				msg = "sold out"
			}
			writeJSON(w, code, map[string]string{"error": msg})
			return
		}
		writeJSON(w, http.StatusCreated, map[string]interface{}{
			"orderId": oid,
			"status":  "PENDING_PAYMENT",
		})
	})

	addr := ":9001"
	logger.Info("backend-demo listening", "addr", addr)
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server error", "err", err)
	}
}

func writeJSON(w http.ResponseWriter, code int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(body)
}
