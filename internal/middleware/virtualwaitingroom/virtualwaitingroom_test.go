package virtualwaitingroom_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/openwar/openwar/internal/middleware/virtualwaitingroom"
	"github.com/openwar/openwar/internal/waitingroom"
	"github.com/redis/go-redis/v9"
)

func setupTestVWR(t *testing.T) (func(http.Handler) http.Handler, *waitingroom.Room, *miniredis.Miniredis) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	room := waitingroom.New(rdb, 15*time.Second, 5*time.Minute)
	mw := virtualwaitingroom.Middleware(room)
	return mw, room, s
}

func TestVirtualWaitingRoom_MissingEventOrSession(t *testing.T) {
	mw, _, _ := setupTestVWR(t)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// 1. Missing event id
	req := httptest.NewRequest(http.MethodPost, "/event//purchase", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for missing event id, got %d", rec.Code)
	}

	// 2. Missing session cookie
	req = httptest.NewRequest(http.MethodPost, "/event/flash-001/purchase", nil)
	req.SetPathValue("id", "flash-001")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for missing session cookie, got %d", rec.Code)
	}
}

func TestVirtualWaitingRoom_AdmissionFlowAndSingleUse(t *testing.T) {
	ctx := context.Background()
	mw, room, _ := setupTestVWR(t)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("checkout passed"))
	}))

	event := "flash-001"
	sid := "test-session-123"

	// 1. Session exists in queue but is NOT admitted -> 403 Forbidden
	room.Join(ctx, event, sid)
	room.Heartbeat(ctx, event, sid)

	req := httptest.NewRequest(http.MethodPost, "/event/flash-001/purchase", nil)
	req.SetPathValue("id", event)
	req.AddCookie(&http.Cookie{Name: "openwar_session", Value: sid})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden when not admitted, got %d", rec.Code)
	}

	// 2. Admit session -> should pass 200 OK and consume token
	room.Admit(ctx, event, sid)

	req2 := httptest.NewRequest(http.MethodPost, "/event/flash-001/purchase", nil)
	req2.SetPathValue("id", event)
	req2.AddCookie(&http.Cookie{Name: "openwar_session", Value: sid})
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK after admission, got %d", rec2.Code)
	}

	// 3. Subsequent request with same session -> should be 403 Forbidden because token was single-use
	req3 := httptest.NewRequest(http.MethodPost, "/event/flash-001/purchase", nil)
	req3.SetPathValue("id", event)
	req3.AddCookie(&http.Cookie{Name: "openwar_session", Value: sid})
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden on second request due to single-use token consumption, got %d", rec3.Code)
	}
}
