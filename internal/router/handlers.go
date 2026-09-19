package router

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/openwar/openwar/internal/middleware"
	"github.com/openwar/openwar/internal/ratelimiter"
	"github.com/openwar/openwar/internal/waitingroom"
)

type roomHandlers struct {
	room        *waitingroom.Room
	limiter     *ratelimiter.Limiter
	heartbeatMs int64
}

func addRoomHandlers(mux *http.ServeMux, base func(http.Handler) http.Handler, room *waitingroom.Room, limiter *ratelimiter.Limiter, heartbeatMs int64) {
	h := &roomHandlers{room: room, limiter: limiter, heartbeatMs: heartbeatMs}

	mux.Handle("POST /event/{id}/queue", base(http.HandlerFunc(h.join)))
	mux.Handle("GET /event/{id}/queue/status", base(http.HandlerFunc(h.status)))
	mux.Handle("POST /event/{id}/queue/heartbeat", base(http.HandlerFunc(h.heartbeat)))
	mux.Handle("POST /event/{id}/admit", base(http.HandlerFunc(h.admit)))
}

func (h *roomHandlers) checkIPRateLimit(w http.ResponseWriter, r *http.Request, event string) bool {
	if h.limiter == nil {
		return true
	}
	ip := clientIP(r)
	res := h.limiter.Allow(r.Context(), fmt.Sprintf("rl:ip:%s:%s", event, ip))
	if !res.Allowed {
		w.Header().Set("Retry-After", strconv.Itoa(int(res.RetryAfter.Seconds())))
		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(int(h.limiter.Capacity())))
		w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(int(res.Remaining)))
		writeJSON(w, http.StatusTooManyRequests, map[string]string{
			"error": "too many queue requests from this IP",
		})
		return false
	}
	return true
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if idx := strings.Index(xff, ","); idx != -1 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func (h *roomHandlers) join(w http.ResponseWriter, r *http.Request) {
	event := r.PathValue("id")
	if !h.checkIPRateLimit(w, r, event) {
		return
	}

	sid := middleware.SessionIDFrom(r)
	if sid == "" {
		sid = newSID()
		middleware.SetSessionID(w, sid)
	}

	pos, err := h.room.Join(r.Context(), event, sid)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "queue unavailable"})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"roomId":    event,
		"sessionID": sid,
		"position":  pos,
		"admitted":  false,
	})
}

func (h *roomHandlers) status(w http.ResponseWriter, r *http.Request) {
	event := r.PathValue("id")
	sid := middleware.SessionIDFrom(r)
	if sid == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing session"})
		return
	}
	pos, admitted, err := h.room.Status(r.Context(), event, sid)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "queue unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"roomId":   event,
		"position": pos,
		"admitted": admitted,
	})
}

func (h *roomHandlers) heartbeat(w http.ResponseWriter, r *http.Request) {
	event := r.PathValue("id")
	sid := middleware.SessionIDFrom(r)
	if sid == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing session"})
		return
	}
	pos, admitted, err := h.room.Heartbeat(r.Context(), event, sid)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "queue unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"position":            pos,
		"admitted":            admitted,
		"heartbeatIntervalMs": h.heartbeatMs,
	})
}

func (h *roomHandlers) admit(w http.ResponseWriter, r *http.Request) {
	event := r.PathValue("id")
	sid := middleware.SessionIDFrom(r)
	if sid == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing session"})
		return
	}
	ok, err := h.room.ConsumeAdmission(r.Context(), event, sid)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "admit unavailable"})
		return
	}
	if !ok {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "no admission token"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"admitted": true})
}

func newSID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "sid-" + hex.EncodeToString([]byte("fallback"))
	}
	return hex.EncodeToString(b)
}

func writeJSON(w http.ResponseWriter, code int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(body)
}
