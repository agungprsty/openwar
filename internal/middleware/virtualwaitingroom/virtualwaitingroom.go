package virtualwaitingroom

import (
	"net/http"

	"github.com/openwar/openwar/internal/middleware"
	"github.com/openwar/openwar/internal/waitingroom"
)

// Middleware validates that the session holds a live, unexpired admission token
// before allowing a checkout-path request through. This stops token-sharing
// and replay.
func Middleware(room *waitingroom.Room) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			event := r.PathValue("event")
			if event == "" {
				event = r.PathValue("id")
			}
			if event == "" {
				middleware.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "missing event id"})
				return
			}

			sid := sessionID(r)
			if sid == "" {
				middleware.WriteJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing session"})
				return
			}

			ok, err := room.ConsumeAdmission(r.Context(), event, sid)
			if err != nil || !ok {
				middleware.WriteJSON(w, http.StatusForbidden, map[string]string{
					"error":  "not admitted to checkout",
					"roomId": event,
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// sessionID reads the session cookie (set by the queue page on join).
func sessionID(r *http.Request) string {
	return middleware.SessionIDFrom(r)
}
