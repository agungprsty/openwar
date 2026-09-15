package tokenbucket

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/openwar/openwar/internal/metrics"
	"github.com/openwar/openwar/internal/middleware"
	"github.com/openwar/openwar/internal/ratelimiter"
)

// Middleware applies the Redis-backed token bucket to the request using a
// per-event (or per-route) key so capacity is scoped correctly. Metrics are
// recorded with the event label only, keeping Prometheus cardinality bounded.
func Middleware(lim *ratelimiter.Limiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			event := pathEvent(r)
			key := bucketKey(event, middleware.UserIDFrom(r))

			res := lim.Allow(r.Context(), key)
			if res.Err != nil {
				// Redis hiccup → note it and proceed per FailStrategy.
				metrics.RateLimiterErrors.WithLabelValues(event).Inc()
			}
			metrics.TokensRemaining.WithLabelValues(event).Set(float64(res.Remaining))
			if !res.Allowed {
				w.Header().Set("Retry-After", strconv.Itoa(int(res.RetryAfter.Seconds())))
				w.Header().Set("X-RateLimit-Limit", strconv.Itoa(int(lim.Capacity())))
				w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(int(res.Remaining)))
				metrics.RateLimitedTotal.WithLabelValues(event).Inc()
				middleware.WriteJSON(w, http.StatusTooManyRequests, map[string]string{
					"error": "rate limit exceeded",
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func pathEvent(r *http.Request) string {
	if e := r.PathValue("event"); e != "" {
		return e
	}
	if e := r.PathValue("id"); e != "" {
		return e
	}
	return "default"
}

// bucketKey keeps consumers sharing one bucket when we don't scope per user.
func bucketKey(event, userID string) string {
	if userID == "" {
		return fmt.Sprintf("{event:%s}:bucket", event)
	}
	return fmt.Sprintf("{event:%s}:bucket:%s", event, userID)
}

// BucketKey is used by the proxy to rewrite per-request context.
func BucketKey(event, userID string) string {
	return bucketKey(event, userID)
}
