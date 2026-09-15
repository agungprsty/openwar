package middleware

import "net/http"

const SessionCookie = "openwar_session"

// SessionIDFrom returns the session ID cookie value if present.
func SessionIDFrom(r *http.Request) string {
	if c, err := r.Cookie(SessionCookie); err == nil {
		return c.Value
	}
	return ""
}

// SetSessionID stamps the session cookie on the response.
func SetSessionID(w http.ResponseWriter, sid string) {
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookie,
		Value:    sid,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   6 * 3600,
	})
}
