package middleware

import "net/http"

// CORSMiddleware menambahkan header CORS ke semua response.
// Mendukung preflight (OPTIONS) agar browser bisa pre-check sebelum request aktual.
//
// Terima satu atau banyak allowed origin:
//   - CORSMiddleware("*")                            -- izinkan semua
//   - CORSMiddleware("https://app.example.com")      -- satu origin
//   - CORSMiddleware("https://app.com", "https://staging.app.com")  -- whitelist
//
// Access-Control-Allow-Origin hanya bisa satu nilai, jadi origin yang cocok
// di-echo balik. Kalau tidak ada yang cocok, header tidak di-set (browser blokir).
func CORSMiddleware(allowedOrigins ...string) func(http.Handler) http.Handler {
	// Bangun set untuk O(1) lookup.
	set := make(map[string]struct{}, len(allowedOrigins))
	wildcard := false
	for _, o := range allowedOrigins {
		if o == "*" {
			wildcard = true
		}
		set[o] = struct{}{}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")

			if origin != "" {
				allowed := wildcard
				if !allowed {
					_, allowed = set[origin]
				}

				if allowed {
					// Echo origin yang spesifik agar Vary di-set dan
					// browser tidak cache response untuk origin lain.
					w.Header().Set("Vary", "Origin")
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
					w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Access-Token, X-Guest-Session-ID, X-Payment-Token")
					w.Header().Set("Access-Control-Allow-Credentials", "true")
					w.Header().Set("Access-Control-Max-Age", "86400")
				}
			}

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
