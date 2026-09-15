package middleware

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

type Claims struct {
	UserID  string `json:"uid"`
	EventID string `json:"eid,omitempty"`
	jwt.RegisteredClaims
}

const UserIDCtx CtxKey = "userID"

// JWT validates the Authorization bearer token and stashes the user ID.
// Tokens without a uid claim are rejected: every protected call must be
// attributable to a concrete user.
func JWT(secret []byte) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ah := r.Header.Get("Authorization")
			tokenStr, ok := strings.CutPrefix(ah, "Bearer ")
			if !ok || tokenStr == "" {
				writeError(w, http.StatusUnauthorized, "missing bearer token")
				return
			}

			claims := &Claims{}
			token, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
				if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
				}
				return secret, nil
			})
			if err != nil || !token.Valid {
				writeError(w, http.StatusUnauthorized, "invalid token")
				return
			}
			if claims.UserID == "" {
				writeError(w, http.StatusUnauthorized, "token missing uid claim")
				return
			}

			ctx := setCtx(r.Context(), UserIDCtx, claims.UserID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// UserIDFrom returns the authenticated user ID or "" for unauthenticated routes.
func UserIDFrom(r *http.Request) string {
	if v, ok := r.Context().Value(UserIDCtx).(string); ok {
		return v
	}
	return ""
}
