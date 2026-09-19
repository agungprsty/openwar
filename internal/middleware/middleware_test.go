package middleware_test

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/openwar/openwar/internal/middleware"
)

func generateTestJWT(secret []byte, uid, eid string, exp time.Duration) string {
	claims := &middleware.Claims{
		UserID:  uid,
		EventID: eid,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(exp)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString(secret)
	return tokenString
}

func TestJWT_Middleware(t *testing.T) {
	secret := []byte("my-test-secret")
	mw := middleware.JWT(secret)

	var capturedUser string
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedUser = middleware.UserIDFrom(r)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))

	// 1. Missing header -> 401
	req1 := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for missing token, got %d", rec1.Code)
	}

	// 2. Invalid bearer format -> 401
	req2 := httptest.NewRequest(http.MethodGet, "/test", nil)
	req2.Header.Set("Authorization", "Basic 12345")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for non-Bearer token, got %d", rec2.Code)
	}

	// 3. Invalid token signature -> 401
	badToken := generateTestJWT([]byte("wrong-secret"), "user-1", "event-1", 1*time.Hour)
	req3 := httptest.NewRequest(http.MethodGet, "/test", nil)
	req3.Header.Set("Authorization", "Bearer "+badToken)
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for bad signature, got %d", rec3.Code)
	}

	// 4. Valid token -> 200 OK and captures UserID
	validToken := generateTestJWT(secret, "user-99", "event-1", 1*time.Hour)
	req4 := httptest.NewRequest(http.MethodGet, "/test", nil)
	req4.Header.Set("Authorization", "Bearer "+validToken)
	rec4 := httptest.NewRecorder()
	handler.ServeHTTP(rec4, req4)
	if rec4.Code != http.StatusOK {
		t.Errorf("expected 200 for valid token, got %d", rec4.Code)
	}
	if capturedUser != "user-99" {
		t.Errorf("expected captured user 'user-99', got %q", capturedUser)
	}
}

func TestCORS_Middleware(t *testing.T) {
	mw := middleware.CORS([]string{"https://example.com"})

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Preflight OPTIONS request
	req := httptest.NewRequest(http.MethodOptions, "/test", nil)
	req.Header.Set("Origin", "https://example.com")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("expected 204 No Content on preflight OPTIONS, got %d", rec.Code)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "https://example.com" {
		t.Errorf("expected Access-Control-Allow-Origin header set, got %q", rec.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestRequestID_Middleware(t *testing.T) {
	mw := middleware.RequestID()

	var reqIDInCtx string
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqIDInCtx = middleware.RequestIDFrom(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Header().Get("X-Request-ID") == "" {
		t.Error("expected X-Request-ID response header")
	}
	if reqIDInCtx == "" {
		t.Error("expected request ID in context")
	}
}

func TestRecovery_Middleware(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mw := middleware.Recovery(logger)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(fmt.Errorf("fatal unexpected panic"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	rec := httptest.NewRecorder()

	// Should not crash the process, should return 500
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on recovered panic, got %d", rec.Code)
	}
}
