package httpx

import (
	"encoding/json"
	"net/http"
)

// HandlerFunc is a function that handles HTTP requests and may return an error
type HandlerFunc func(w http.ResponseWriter, r *http.Request) error

// Wrap converts a HandlerFunc to an http.HandlerFunc by handling errors
func Wrap(fn HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			Error(w, http.StatusInternalServerError, err.Error())
		}
	}
}

// JSON writes a JSON response with the given status code
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

// Error writes a JSON error response with the given status code and message
func Error(w http.ResponseWriter, status int, msg string) {
	JSON(w, status, map[string]string{"error": msg})
}
