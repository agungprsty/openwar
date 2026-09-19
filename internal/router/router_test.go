package router_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/openwar/openwar/internal/ratelimiter"
	"github.com/openwar/openwar/internal/router"
	"github.com/openwar/openwar/internal/waitingroom"
	"github.com/redis/go-redis/v9"
)

func setupTestRouter(t *testing.T) (*http.ServeMux, *waitingroom.Room, *miniredis.Miniredis) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	room := waitingroom.New(rdb, 15*time.Second, 5*time.Minute)
	limiter := ratelimiter.New(rdb, 100, 50, ratelimiter.FailOpen)
	backendURL, _ := url.Parse("http://localhost:9001")

	deps := router.Deps{
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		JWTSecret:      []byte("test-secret"),
		AllowedOrigins: []string{"*"},
		RDB:            rdb,
		Room:           room,
		Limiter:        limiter,
		IdemWindow:     24 * time.Hour,
		HeartbeatMs:    5000,
		BackendURL:     backendURL,
	}

	return router.New(deps), room, s
}

func TestRouter_Healthz(t *testing.T) {
	mux, _, s := setupTestRouter(t)

	// 1. Healthy
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK on /healthz, got %d", rec.Code)
	}

	var healthResp struct {
		Status     string            `json:"status"`
		Components map[string]string `json:"components"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&healthResp); err != nil {
		t.Fatalf("failed to decode health response: %v", err)
	}
	if healthResp.Status != "UP" || healthResp.Components["redis"] != "UP" {
		t.Errorf("expected status UP, got %+v", healthResp)
	}

	// 2. Unhealthy when Redis is down
	s.Close()
	req2 := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 Service Unavailable on dead redis, got %d", rec2.Code)
	}
}

func TestRouter_IPRateLimit(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	room := waitingroom.New(rdb, 15*time.Second, 5*time.Minute)
	limiter := ratelimiter.New(rdb, 2, 0.1, ratelimiter.FailClosed) // capacity 2
	backendURL, _ := url.Parse("http://localhost:9001")

	deps := router.Deps{
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		JWTSecret:      []byte("test-secret"),
		AllowedOrigins: []string{"*"},
		RDB:            rdb,
		Room:           room,
		Limiter:        limiter,
		IdemWindow:     24 * time.Hour,
		HeartbeatMs:    5000,
		BackendURL:     backendURL,
	}

	mux := router.New(deps)

	// Call 1 -> OK
	req1 := httptest.NewRequest(http.MethodPost, "/event/flash-001/queue", nil)
	req1.RemoteAddr = "192.168.1.50:12345"
	rec1 := httptest.NewRecorder()
	mux.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusAccepted {
		t.Fatalf("expected 202 on call 1, got %d", rec1.Code)
	}

	// Call 2 -> OK
	req2 := httptest.NewRequest(http.MethodPost, "/event/flash-001/queue", nil)
	req2.RemoteAddr = "192.168.1.50:12345"
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusAccepted {
		t.Fatalf("expected 202 on call 2, got %d", rec2.Code)
	}

	// Call 3 -> 429 Too Many Requests from same IP
	req3 := httptest.NewRequest(http.MethodPost, "/event/flash-001/queue", nil)
	req3.RemoteAddr = "192.168.1.50:12345"
	rec3 := httptest.NewRecorder()
	mux.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429 on call 3 from same IP, got %d", rec3.Code)
	}
}

func TestRouter_QueueJoinAndHeartbeatFlow(t *testing.T) {
	mux, _, _ := setupTestRouter(t)

	// 1. Join queue without cookie
	reqJoin := httptest.NewRequest(http.MethodPost, "/event/flash-001/queue", nil)
	recJoin := httptest.NewRecorder()
	mux.ServeHTTP(recJoin, reqJoin)

	if recJoin.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted on join, got %d: %s", recJoin.Code, recJoin.Body.String())
	}

	var joinResp struct {
		RoomID    string `json:"roomId"`
		SessionID string `json:"sessionID"`
		Position  int64  `json:"position"`
		Admitted  bool   `json:"admitted"`
	}
	if err := json.NewDecoder(recJoin.Body).Decode(&joinResp); err != nil {
		t.Fatalf("failed to parse join response: %v", err)
	}

	if joinResp.RoomID != "flash-001" {
		t.Errorf("expected roomId 'flash-001', got %q", joinResp.RoomID)
	}
	if joinResp.SessionID == "" {
		t.Errorf("expected non-empty sessionID")
	}
	if joinResp.Position != 0 {
		t.Errorf("expected initial position 0, got %d", joinResp.Position)
	}

	// Verify Set-Cookie header is returned
	cookies := recJoin.Result().Cookies()
	var sessionCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == "openwar_session" {
			sessionCookie = c
			break
		}
	}
	if sessionCookie == nil {
		t.Fatalf("expected openwar_session cookie to be set")
	}

	// 2. Status without cookie -> 401
	reqStatusNoCookie := httptest.NewRequest(http.MethodGet, "/event/flash-001/queue/status", nil)
	recStatusNoCookie := httptest.NewRecorder()
	mux.ServeHTTP(recStatusNoCookie, reqStatusNoCookie)
	if recStatusNoCookie.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized without session cookie, got %d", recStatusNoCookie.Code)
	}

	// 3. Status with cookie -> 200
	reqStatus := httptest.NewRequest(http.MethodGet, "/event/flash-001/queue/status", nil)
	reqStatus.AddCookie(sessionCookie)
	recStatus := httptest.NewRecorder()
	mux.ServeHTTP(recStatus, reqStatus)
	if recStatus.Code != http.StatusOK {
		t.Errorf("expected 200 OK with session cookie, got %d", recStatus.Code)
	}

	// 4. Heartbeat with cookie -> 200
	reqHB := httptest.NewRequest(http.MethodPost, "/event/flash-001/queue/heartbeat", nil)
	reqHB.AddCookie(sessionCookie)
	recHB := httptest.NewRecorder()
	mux.ServeHTTP(recHB, reqHB)
	if recHB.Code != http.StatusOK {
		t.Errorf("expected 200 OK for heartbeat, got %d", recHB.Code)
	}
}

func TestRouter_PurchaseProtectionWithoutAuth(t *testing.T) {
	mux, _, _ := setupTestRouter(t)

	// Direct purchase without token -> 401 Unauthorized
	req := httptest.NewRequest(http.MethodPost, "/event/flash-001/purchase", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized on purchase without JWT, got %d", rec.Code)
	}
}
