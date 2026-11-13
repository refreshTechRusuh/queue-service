package middleware

import "net/http"

// Nilai header CORS yang sama untuk semua origin yang diizinkan.
const (
	corsAllowMethods = "GET, POST, PUT, DELETE, OPTIONS"
	corsAllowHeaders = "Content-Type, Authorization, X-Access-Token, X-Guest-Session-ID, X-Payment-Token, X-Transaction-Id, Cache-Control, Last-Event-ID, Accept"
	corsMaxAge       = "86400"
)

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
	isAllowed := originMatcher(allowedOrigins)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if origin := r.Header.Get("Origin"); origin != "" && isAllowed(origin) {
				setCORSHeaders(w.Header(), origin)
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

// originMatcher membangun pemeriksa origin dengan lookup O(1); "*" mengizinkan semua.
func originMatcher(allowedOrigins []string) func(string) bool {
	set := make(map[string]struct{}, len(allowedOrigins))
	for _, o := range allowedOrigins {
		if o == "*" {
			return func(string) bool { return true }
		}
		set[o] = struct{}{}
	}
	return func(origin string) bool {
		_, ok := set[origin]
		return ok
	}
}

// setCORSHeaders meng-echo origin yang spesifik (bukan "*") supaya Vary di-set
// dan browser tidak memakai cache response untuk origin lain.
func setCORSHeaders(h http.Header, origin string) {
	h.Set("Vary", "Origin")
	h.Set("Access-Control-Allow-Origin", origin)
	h.Set("Access-Control-Allow-Methods", corsAllowMethods)
	h.Set("Access-Control-Allow-Headers", corsAllowHeaders)
	h.Set("Access-Control-Allow-Credentials", "true")
	h.Set("Access-Control-Max-Age", corsMaxAge)
}
