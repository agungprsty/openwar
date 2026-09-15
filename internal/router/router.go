package router

import (
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

	mux.Handle("GET /healthz", base(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})))

	// Waiting-room endpoints bypass JWT + idempotency (cookie-session based).
	addRoomHandlers(mux, base, deps.Room, deps.HeartbeatMs)

	return mux
}
