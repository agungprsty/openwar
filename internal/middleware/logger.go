package middleware

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/openwar/openwar/internal/metrics"
)

type statusRecorder struct {
	w      http.ResponseWriter
	status int
	bytes  int
}

func (r *statusRecorder) Header() http.Header {
	return r.w.Header()
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.w.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.w.Write(b)
	r.bytes += n
	return n, err
}

// Logger adds structured request logging and core request metrics.
func Logger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{w: w}
			next.ServeHTTP(rec, r)
			if rec.status == 0 {
				rec.status = http.StatusOK
			}
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			logger.Info("http_request",
				"request_id", RequestIDFrom(r.Context()),
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"bytes", rec.bytes,
				"dur_ms", time.Since(start).Milliseconds(),
			)
			metrics.RequestsTotal.WithLabelValues(r.Method, route, http.StatusText(rec.status)).Inc()
			metrics.RequestDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
		})
	}
}
