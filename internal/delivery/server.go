// Package delivery adalah composition root: rakit semua lapis + daftarkan route.
package delivery

import (
	"net/http"

	goredis "github.com/redis/go-redis/v9"

	"github.com/refreshTechRusuh/queue-service/internal/config"
	"github.com/refreshTechRusuh/queue-service/pkg/middleware"
)

// New merakit dependency dan mengembalikan handler (mux dibungkus middleware global).
// Skeleton: baru health check. Domain queue dirakit di sini pada fase fitur.
func New(_ *config.Config, rdb *goredis.Client) http.Handler {
	mux := http.NewServeMux()

	// Health: ikut cek Redis karena state antrian sepenuhnya ada di Redis.
	mux.HandleFunc("GET /sys/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := rdb.Ping(r.Context()).Err(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"503","message":"redis unavailable","data":{}}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"00000","message":"queue-service ok","data":{}}`))
	})

	return middleware.Chain(mux, middleware.TransactionID)
}
