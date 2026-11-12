package middleware

import (
	"net/http"
	"strconv"
)

// DefaultMaxBodyBytes = batas ukuran body request bila MAX_BODY_BYTES tidak diatur.
// Semua body API ini JSON kecil (login, member, join antrian, booking), jadi 1 MB sudah longgar.
const DefaultMaxBodyBytes int64 = 1 << 20

const bodyTooLarge = `{"status":"413","message":"Ukuran request terlalu besar","data":{}}`

// SecurityHeaders memasang header pengaman standar API JSON di SETIAP response.
//
//   - X-Content-Type-Options: nosniff     browser tidak boleh menebak tipe konten
//   - Strict-Transport-Security           paksa HTTPS (diabaikan browser bila via HTTP)
//   - Content-Security-Policy: default-src 'none'   respons API tidak memuat resource apa pun
//   - Referrer-Policy: no-referrer        URL tidak bocor ke situs lain
//   - Cache-Control: no-store             DEFAULT aman: data login/pribadi tidak di-cache
//
// Cache-Control diatur SEBELUM handler berjalan, sehingga handler yang memang boleh
// di-cache (atau SSE yang butuh "no-cache") tinggal menimpanya dengan Header().Set.
//
// SENGAJA tanpa X-Frame-Options / frame-ancestors: tidak mempersulit iframe pembayaran FE.
func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		h.Set("Content-Security-Policy", "default-src 'none'")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

// MaxBody membatasi ukuran body request.
//
//   - Content-Length sudah melebihi batas  -> langsung 413 (handler tidak dijalankan)
//   - Content-Length tidak diketahui (chunked) atau berbohong -> body dibungkus
//     http.MaxBytesReader; pembacaan melewati batas gagal, dan handler yang men-decode
//     JSON akan menjawab 400 "body tidak valid".
//
// limit <= 0 memakai DefaultMaxBodyBytes (tidak pernah "tanpa batas" secara tak sengaja).
func MaxBody(limit int64) func(http.Handler) http.Handler {
	if limit <= 0 {
		limit = DefaultMaxBodyBytes
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ContentLength > limit {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Connection", "close")
				w.Header().Set("Content-Length", strconv.Itoa(len(bodyTooLarge)))
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				_, _ = w.Write([]byte(bodyTooLarge))
				return
			}
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, limit)
			}
			next.ServeHTTP(w, r)
		})
	}
}
