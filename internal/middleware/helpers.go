package middleware

import (
	"context"
	"encoding/json"
	"net/http"
)

func setCtx(ctx context.Context, key CtxKey, val string) context.Context {
	return context.WithValue(ctx, key, val)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, code int, body interface{}) {
	WriteJSON(w, code, body)
}

// WriteJSON writes a JSON body with the given status code.
func WriteJSON(w http.ResponseWriter, code int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(body)
}
