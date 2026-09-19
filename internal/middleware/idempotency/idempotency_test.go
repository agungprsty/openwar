package idempotency_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/openwar/openwar/internal/middleware/idempotency"
	"github.com/redis/go-redis/v9"
)

func setupTestIdem(t *testing.T) (func(http.Handler) http.Handler, *miniredis.Miniredis, *redis.Client) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	mw := idempotency.Middleware(rdb, 1*time.Hour)
	return mw, s, rdb
}

func TestIdempotency_MissingOrInvalidKey(t *testing.T) {
	mw, _, _ := setupTestIdem(t)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Missing header
	req := httptest.NewRequest(http.MethodPost, "/event/flash-001/purchase", bytes.NewReader([]byte(`{"qty":1}`)))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for missing key, got %d", rec.Code)
	}

	// Invalid UUID
	req = httptest.NewRequest(http.MethodPost, "/event/flash-001/purchase", bytes.NewReader([]byte(`{"qty":1}`)))
	req.Header.Set("Idempotency-Key", "not-a-uuid")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for invalid UUID, got %d", rec.Code)
	}
}

func TestIdempotency_BypassNonPost(t *testing.T) {
	mw, _, _ := setupTestIdem(t)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("get response"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/event/flash-001/purchase", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK on GET, got %d", rec.Code)
	}
}

func TestIdempotency_FirstCallAndReplay(t *testing.T) {
	mw, _, _ := setupTestIdem(t)

	calls := 0
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"orderId":"ord-123","status":"PENDING"}`))
	}))

	validUUID := "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"
	bodyPayload := `{"sku":"flash-001:ticket","qty":1}`

	// 1. First execution
	req1 := httptest.NewRequest(http.MethodPost, "/event/flash-001/purchase", bytes.NewReader([]byte(bodyPayload)))
	req1.SetPathValue("id", "flash-001")
	req1.Header.Set("Idempotency-Key", validUUID)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created on first request, got %d", rec1.Code)
	}
	if calls != 1 {
		t.Fatalf("expected 1 handler call, got %d", calls)
	}

	// 2. Replay with identical payload and key -> Should NOT invoke handler, returns cached response
	req2 := httptest.NewRequest(http.MethodPost, "/event/flash-001/purchase", bytes.NewReader([]byte(bodyPayload)))
	req2.SetPathValue("id", "flash-001")
	req2.Header.Set("Idempotency-Key", validUUID)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusCreated {
		t.Errorf("expected 201 Created on replay, got %d", rec2.Code)
	}
	if rec2.Header().Get("X-Idempotent-Replay") != "true" {
		t.Errorf("expected X-Idempotent-Replay header to be 'true'")
	}
	if calls != 1 {
		t.Errorf("expected handler NOT to be called again, but calls = %d", calls)
	}

	// 3. Replay with DIFFERENT payload -> 422 Unprocessable Entity
	req3 := httptest.NewRequest(http.MethodPost, "/event/flash-001/purchase", bytes.NewReader([]byte(`{"sku":"other:ticket","qty":2}`)))
	req3.SetPathValue("id", "flash-001")
	req3.Header.Set("Idempotency-Key", validUUID)
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusUnprocessableEntity {
		t.Errorf("expected 422 Unprocessable Entity on mismatched payload, got %d", rec3.Code)
	}
}

func TestIdempotency_Failed5xxRetryable(t *testing.T) {
	mw, _, _ := setupTestIdem(t)

	attempt := 0
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt++
		if attempt == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error":"temporary upstream failure"}`))
			return
		}
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"orderId":"ord-456"}`))
	}))

	validUUID := "b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e"
	bodyPayload := `{"sku":"flash-001:ticket","qty":1}`

	// 1. First attempt fails with 500
	req1 := httptest.NewRequest(http.MethodPost, "/event/flash-001/purchase", bytes.NewReader([]byte(bodyPayload)))
	req1.SetPathValue("id", "flash-001")
	req1.Header.Set("Idempotency-Key", validUUID)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on first call, got %d", rec1.Code)
	}

	// 2. Retry with same key -> should execute handler again and succeed
	req2 := httptest.NewRequest(http.MethodPost, "/event/flash-001/purchase", bytes.NewReader([]byte(bodyPayload)))
	req2.SetPathValue("id", "flash-001")
	req2.Header.Set("Idempotency-Key", validUUID)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusCreated {
		t.Errorf("expected 201 on retry after 5xx, got %d", rec2.Code)
	}
	if attempt != 2 {
		t.Errorf("expected 2 handler attempts, got %d", attempt)
	}
}
