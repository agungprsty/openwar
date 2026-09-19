package idempotency

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/openwar/openwar/internal/lua"
	"github.com/openwar/openwar/internal/metrics"
	"github.com/openwar/openwar/internal/middleware"
	"github.com/redis/go-redis/v9"
)

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

const idemKeyCtx middleware.CtxKey = "idemKey"

// maxBodyBytes caps the request body we buffer for fingerprinting. Purchase
// payloads are a few hundred bytes; anything larger is rejected outright.
const maxBodyBytes = 4 << 10

// maxCachedResponse caps the replayed response body stored in Redis.
const maxCachedResponse = 1 << 20

// Middleware enforces client-generated idempotency keys: only the first
// request carrying a key executes; replays get a cached response or 409.
func Middleware(rdb *redis.Client, window time.Duration) func(http.Handler) http.Handler {
	claim := redis.NewScript(lua.ClaimIdem)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				next.ServeHTTP(w, r)
				return
			}

			key := r.Header.Get("Idempotency-Key")
			if key == "" {
				middleware.WriteJSON(w, http.StatusBadRequest, map[string]string{
					"error": "Idempotency-Key header required",
				})
				return
			}
			if !uuidV4.MatchString(key) {
				middleware.WriteJSON(w, http.StatusBadRequest, map[string]string{
					"error": "Idempotency-Key must be a UUID v4",
				})
				return
			}

			// Buffer the (capped) body for deterministic fingerprinting.
			r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
			body, err := io.ReadAll(r.Body)
			if err != nil {
				var mbe *http.MaxBytesError
				if ok := errors.As(err, &mbe); ok {
					middleware.WriteJSON(w, http.StatusRequestEntityTooLarge, map[string]string{
						"error": "request body too large",
					})
					return
				}
				middleware.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "unreadable body"})
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			fingerprint := string(fmt.Sprintf("%s:%s", r.URL.Path, body))

			event := r.PathValue("event")
			if event == "" {
				event = r.PathValue("id")
			}
			user := middleware.UserIDFrom(r)
			rkey := fmt.Sprintf("idem:%s:%s:%s", user, event, key)

			res, err := claim.Run(r.Context(), rdb, []string{rkey},
				int(window.Seconds()), fingerprint, time.Now().UnixMilli()).Result()
			if err != nil {
				// Redis down → per strategy. Purchases must not double-execute,
				// so fail closed: the client can retry once Redis recovers.
				middleware.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{
					"error": "idempotency store unavailable",
				})
				return
			}

			arr, _ := res.([]interface{})
			status := ""
			if len(arr) >= 2 {
				if s, ok := arr[1].(string); ok {
					status = s
				}
			}

			switch status {
			case "DUPLICATE":
				metrics.IdempotencyReplays.WithLabelValues("conflict").Inc()
				if snap, err := rdb.HGetAll(r.Context(), rkey).Result(); err == nil && snap["respBody"] != "" {
					code, _ := strconv.Atoi(snap["statusCode"])
					w.Header().Set("X-Idempotent-Replay", "true")
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(code)
					w.Write([]byte(snap["respBody"]))
					return
				}
				middleware.WriteJSON(w, http.StatusConflict, map[string]string{
					"error":          "duplicate request",
					"idempotencyKey": key,
				})
				return
			case "MISMATCH":
				metrics.IdempotencyReplays.WithLabelValues("mismatch").Inc()
				middleware.WriteJSON(w, http.StatusUnprocessableEntity, map[string]string{
					"error": "Idempotency-Key reused with a different payload",
				})
				return
			}

			// First time → execute, capture + cache the response.
			ctx := context.WithValue(r.Context(), idemKeyCtx, rkey)
			rec := &responseRecorder{w: w}
			next.ServeHTTP(rec, r.WithContext(ctx))

			if rec.status >= 500 {
				// Nothing committed upstream → mark FAILED so the key stays retryable.
				rdb.HSet(r.Context(), rkey,
					"status", "FAILED",
					"fingerprint", fingerprint,
				)
				rdb.Expire(r.Context(), rkey, window)
				return
			}

			rdb.HSet(r.Context(), rkey,
				"status", "COMPLETED",
				"respBody", string(rec.body),
				"statusCode", strconv.Itoa(rec.status),
			)
			rdb.Expire(r.Context(), rkey, window)
		})
	}
}

type responseRecorder struct {
	w      http.ResponseWriter
	status int
	body   []byte
}

func (r *responseRecorder) Header() http.Header {
	return r.w.Header()
}

func (r *responseRecorder) WriteHeader(code int) {
	r.status = code
	r.w.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	if len(r.body) < maxCachedResponse {
		room := maxCachedResponse - len(r.body)
		if len(b) > room {
			r.body = append(r.body, b[:room]...)
		} else {
			r.body = append(r.body, b...)
		}
	}
	return r.w.Write(b)
}

// KeyFrom returns the idempotency Redis key stamped by Middleware, or "".
func KeyFrom(ctx context.Context) string {
	if v, ok := ctx.Value(idemKeyCtx).(string); ok {
		return v
	}
	return ""
}
