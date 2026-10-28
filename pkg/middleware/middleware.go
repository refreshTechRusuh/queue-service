// Package middleware menyediakan middleware HTTP queue-service.
package middleware

import (
	"net/http"

	"github.com/google/uuid"
)

// TransactionID menambahkan X-Transaction-Id ke response header (untuk tracing).
// Bila klien sudah mengirim header ini, nilainya diteruskan (trace lintas service).
func TransactionID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		txID := r.Header.Get("X-Transaction-Id")
		if txID == "" {
			txID = uuid.NewString()
		}
		w.Header().Set("X-Transaction-Id", txID)
		next.ServeHTTP(w, r)
	})
}

// Chain menyusun middleware dari luar ke dalam (pertama = paling luar).
func Chain(h http.Handler, middlewares ...func(http.Handler) http.Handler) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}
