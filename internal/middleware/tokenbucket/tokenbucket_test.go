package tokenbucket_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/openwar/openwar/internal/middleware/tokenbucket"
	"github.com/openwar/openwar/internal/ratelimiter"
	"github.com/redis/go-redis/v9"
)

func setupTestTB(t *testing.T, cap, refill float64) (func(http.Handler) http.Handler, *miniredis.Miniredis) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	lim := ratelimiter.New(rdb, cap, refill, ratelimiter.FailOpen)
	return tokenbucket.Middleware(lim), s
}

func TestTokenBucket_MiddlewareAllowAndRateLimit(t *testing.T) {
	mw, _ := setupTestTB(t, 2, 0.1) // capacity = 2

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))

	// 1st request -> OK
	req1 := httptest.NewRequest(http.MethodPost, "/event/flash-001/purchase", nil)
	req1.SetPathValue("id", "flash-001")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for 1st request, got %d", rec1.Code)
	}

	// 2nd request -> OK
	req2 := httptest.NewRequest(http.MethodPost, "/event/flash-001/purchase", nil)
	req2.SetPathValue("id", "flash-001")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for 2nd request, got %d", rec2.Code)
	}

	// 3rd request -> 429 Too Many Requests
	req3 := httptest.NewRequest(http.MethodPost, "/event/flash-001/purchase", nil)
	req3.SetPathValue("id", "flash-001")
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429 Too Many Requests for 3rd request, got %d", rec3.Code)
	}
	if rec3.Header().Get("Retry-After") == "" {
		t.Error("expected Retry-After header to be set on 429 response")
	}
	if rec3.Header().Get("X-RateLimit-Limit") != "2" {
		t.Errorf("expected X-RateLimit-Limit to be '2', got %q", rec3.Header().Get("X-RateLimit-Limit"))
	}
}
