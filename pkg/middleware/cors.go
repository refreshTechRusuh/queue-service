package middleware

import "net/http"

// CORSMiddleware menambahkan header CORS ke semua response.
// Mendukung preflight (OPTIONS) agar browser bisa pre-check sebelum request aktual.
//
// allowedOrigin bisa berupa "*" untuk semua origin, atau domain spesifik
// seperti "https://example.com".
func CORSMiddleware(allowedOrigin string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")

			// Tentukan nilai allow-origin: "*" atau echo origin caller
			// jika allowedOrigin dikonfigurasi sebagai "*".
			ao := allowedOrigin
			if ao == "*" && origin != "" {
				ao = origin
				w.Header().Set("Vary", "Origin")
			}

			w.Header().Set("Access-Control-Allow-Origin", ao)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Transaction-Id")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Max-Age", "86400")

			// Preflight: browser kirim OPTIONS sebelum request aktual.
			// Jawab langsung tanpa meneruskan ke handler.
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
