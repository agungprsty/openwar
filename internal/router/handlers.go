package router

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/openwar/openwar/internal/middleware"
	"github.com/openwar/openwar/internal/waitingroom"
)

type roomHandlers struct {
	room        *waitingroom.Room
	heartbeatMs int64
}

func addRoomHandlers(mux *http.ServeMux, base func(http.Handler) http.Handler, room *waitingroom.Room, heartbeatMs int64) {
	h := &roomHandlers{room: room, heartbeatMs: heartbeatMs}

	mux.Handle("POST /event/{id}/queue", base(http.HandlerFunc(h.join)))
	mux.Handle("GET /event/{id}/queue/status", base(http.HandlerFunc(h.status)))
	mux.Handle("POST /event/{id}/queue/heartbeat", base(http.HandlerFunc(h.heartbeat)))
	mux.Handle("POST /event/{id}/admit", base(http.HandlerFunc(h.admit)))
}

func (h *roomHandlers) join(w http.ResponseWriter, r *http.Request) {
	event := r.PathValue("id")
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
