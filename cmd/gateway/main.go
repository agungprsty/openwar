package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/openwar/openwar/internal/config"
	"github.com/openwar/openwar/internal/ratelimiter"
	"github.com/openwar/openwar/internal/router"
	"github.com/openwar/openwar/internal/waitingroom"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if len(os.Args) > 1 && os.Args[1] == "seed-events" {
		runSeed(logger)
		return
	}
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "serve" {
		args = args[1:] // docker default subcommand
	}

	fs := flag.NewFlagSet("gateway", flag.ExitOnError)
	admitRate := fs.Int("admission-rate", 0, "override ADMISSION_RATE")
	fs.Parse(args)

	cfg := config.Load()
	if *admitRate > 0 {
		cfg.AdmissionRate = *admitRate
	}
	if err := cfg.Validate(); err != nil {
		logger.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, PoolSize: 256})
	if err := rdb.Ping(ctx).Err(); err != nil {
		logger.Error("redis unavailable", "err", err)
		os.Exit(1)
	}

	room := waitingroom.New(rdb, cfg.HeartbeatTTL, cfg.AdmissionTTL)

	// Per-event token bucket: capacity = admission rate burst, refill = steady rps.
	limiter := ratelimiter.New(rdb, float64(cfg.AdmissionRate), float64(cfg.AdmissionRate)/float64(2), ratelimiter.FailOpen)

	backendURL, err := url.Parse(cfg.BackendURL)
	if err != nil {
		logger.Error("invalid BACKEND_URL", "err", err)
		os.Exit(1)
	}

	deps := router.Deps{
		Logger:         logger,
		JWTSecret:      []byte(cfg.JWTSecret),
		AllowedOrigins: cfg.AllowedOrigins,
		RDB:            rdb,
		Room:           room,
		Limiter:        limiter,
		IdemWindow:     cfg.IdempotencyWin,
		HeartbeatMs:    cfg.HeartbeatInterval.Milliseconds(),
		BackendURL:     backendURL,
	}

	mux := router.New(deps)

	mux.Handle("GET /metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		promhttp.Handler().ServeHTTP(w, r)
	}))

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		logger.Info("gateway listening", "addr", cfg.ListenAddr, "backend", cfg.BackendURL)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
}
