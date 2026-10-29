package http

import (
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/refreshTechRusuh/queue-service/pkg/apinizer"
	"github.com/refreshTechRusuh/queue-service/pkg/apperror"
)

// JoinRateLimit = batas join per IP per menit (fixed window di Redis, berlaku lintas instance).
//
// Join antrian tanpa identitas apa pun (sesuai flow bisnis), jadi satu-satunya penanda
// di titik antre adalah IP — tempat paling rawan spam. Keadilan di titik beli dijaga
// booking-service (1 tiket per NIK); rate limit ini menjaga sumber daya di titik antre.
// Bila Redis error → fail-open (journey tidak boleh putus karena limiter).
func JoinRateLimit(rdb *redis.Client, perMinute int) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if perMinute <= 0 {
			return next // dimatikan
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			window := time.Now().Unix() / 60
			key := "ratelimit:join:" + clientIP(r) + ":" + strconv.FormatInt(window, 10)

			n, err := rdb.Incr(r.Context(), key).Result()
			if err != nil {
				slog.Warn("rate limit: redis error, fail-open", "err", err)
				next.ServeHTTP(w, r)
				return
			}
			if n == 1 {
				_ = rdb.Expire(r.Context(), key, 70*time.Second).Err()
			}
			if n > int64(perMinute) {
				w.Header().Set("Retry-After", "60")
				apinizer.WriteError(w, apperror.RateLimited())
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// clientIP: Cloud Run meneruskan IP asli di X-Forwarded-For (nilai pertama).
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if ip := strings.TrimSpace(strings.Split(xff, ",")[0]); ip != "" {
			return ip
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
