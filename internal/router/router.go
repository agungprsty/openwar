package router

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/openwar/openwar/internal/middleware"
	"github.com/openwar/openwar/internal/middleware/idempotency"
	"github.com/openwar/openwar/internal/middleware/tokenbucket"
	"github.com/openwar/openwar/internal/middleware/virtualwaitingroom"
	"github.com/openwar/openwar/internal/proxy"
	"github.com/openwar/openwar/internal/ratelimiter"
	"github.com/openwar/openwar/internal/waitingroom"
	"github.com/redis/go-redis/v9"
	"time"
)

type Deps struct {
	Logger         *slog.Logger
	JWTSecret      []byte
	AllowedOrigins []string
	RDB            *redis.Client
	Room           *waitingroom.Room
	Limiter        *ratelimiter.Limiter
	IdemWindow     time.Duration
	HeartbeatMs    int64
	BackendURL     *url.URL
}

// New builds the ServeMux with the protected middleware chain per-route.
func New(deps Deps) *http.ServeMux {
	mux := http.NewServeMux()

	base := middleware.Chain(
		middleware.Recovery(deps.Logger),
		middleware.RequestID(),
		middleware.Logger(deps.Logger),
		middleware.CORS(deps.AllowedOrigins),
	)

	protected := middleware.Chain(
		middleware.JWT(deps.JWTSecret),
		virtualwaitingroom.Middleware(deps.Room),
		tokenbucket.Middleware(deps.Limiter),
		idempotency.Middleware(deps.RDB, deps.IdemWindow),
	)

	handler := base(protected(proxy.Forward(deps.BackendURL)))
	mux.Handle("POST /event/{id}/purchase", handler)

	mux.Handle("GET /healthz", base(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		components := make(map[string]string)
		isHealthy := true

		if deps.RDB != nil {
			if err := deps.RDB.Ping(ctx).Err(); err != nil {
				components["redis"] = "DOWN: " + err.Error()
				isHealthy = false
			} else {
				components["redis"] = "UP"
			}
		}

		status := "UP"
		statusCode := http.StatusOK
		if !isHealthy {
			status = "DOWN"
			statusCode = http.StatusServiceUnavailable
		}

		writeJSON(w, statusCode, map[string]interface{}{
			"status":     status,
			"components": components,
		})
	})))

	// Waiting-room endpoints bypass JWT + idempotency (cookie-session based).
	addRoomHandlers(mux, base, deps.Room, deps.Limiter, deps.HeartbeatMs)

	return mux
}
