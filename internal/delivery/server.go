// Package delivery adalah composition root: rakit semua lapis + daftarkan route
// + jalankan proses latar (hub SSE, sweeper, consumer Kafka).
package delivery

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"

	"github.com/refreshTechRusuh/queue-service/internal/config"
	"github.com/refreshTechRusuh/queue-service/internal/queue/application/usecase"
	"github.com/refreshTechRusuh/queue-service/internal/queue/infrastructure/cache"
	"github.com/refreshTechRusuh/queue-service/internal/queue/infrastructure/db"
	"github.com/refreshTechRusuh/queue-service/internal/queue/infrastructure/event"
	queuehttp "github.com/refreshTechRusuh/queue-service/internal/queue/infrastructure/http"
	"github.com/refreshTechRusuh/queue-service/internal/queue/infrastructure/token"
	"github.com/refreshTechRusuh/queue-service/pkg/kafka"
	"github.com/refreshTechRusuh/queue-service/pkg/middleware"
)

// New merakit dependency, menjalankan proses latar (terikat ctx), dan
// mengembalikan handler HTTP + fungsi penutup.
func New(ctx context.Context, cfg *config.Config, rdb *goredis.Client, pool *pgxpool.Pool) (http.Handler, func()) {
	log := slog.Default()

	// Adapter
	store := cache.NewQueueStore(rdb, cache.StoreSettings{
		GlobalBookingCap: cfg.Queue.GlobalBookingCap,
		QueueCap:         cfg.Queue.QueueCap,
		BookingTTL:       cfg.Queue.BookingTTL,
		StateTTL:         cfg.Queue.StateTTL,
		ClosedTTL:        cfg.Queue.ClosedTTL,
	})
	hub := cache.NewHub(rdb)
	notifier := cache.NewNotifier(rdb)
	signer := token.NewSigner(cfg.JWT.AccessTokenSecret)
	stockSource := db.NewStockSource(pool)

	// Usecase
	uc := usecase.NewQueueUsecase(store, stockSource, notifier, signer,
		usecase.Settings{BookingTTL: cfg.Queue.BookingTTL}, log)

	// Proses latar
	go hub.Run(ctx)
	go runSweeper(ctx, uc.Sweep, cfg.Queue.SweepInterval, log)

	closeFn := func() {}
	if cfg.Kafka.Enabled {
		consumer := event.NewConsumer(uc, kafka.Config{
			Brokers:  cfg.Kafka.Brokers,
			Username: cfg.Kafka.Username,
			Password: cfg.Kafka.Password,
			TLS:      cfg.Kafka.TLS,
			GroupID:  cfg.Kafka.GroupID,
		}, log)
		consumer.Start(ctx)
		closeFn = consumer.Close
	} else {
		log.Warn("KAFKA_ENABLED=false: event BE-2 tidak dikonsumsi (hanya untuk dev)")
	}

	// HTTP
	h := queuehttp.NewHandler(uc, hub, cfg.Queue.SSETick)
	joinLimit := queuehttp.JoinRateLimit(rdb, cfg.Queue.JoinRatePerMin)

	mux := http.NewServeMux()
	mux.Handle("POST /api/queue/v1/event/{eventId}/join", joinLimit(http.HandlerFunc(h.Join)))
	mux.HandleFunc("GET /api/queue/v1/event/{eventId}/{queueToken}", h.Status)
	mux.HandleFunc("GET /api/queue/v1/event/{eventId}/{queueToken}/stream", h.Stream)

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

	// Middleware global (luar -> dalam): header keamanan, CORS, batas body, tracing, transaction-id.
	// Header keamanan paling luar agar ada di semua response (termasuk preflight & 413);
	// batas body di dalam CORS agar 413 tetap membawa header CORS.
	// CORS di luar tracing agar preflight tidak membuat span. /stream (SSE berjam-jam)
	// dan /sys/ping dikecualikan: span SSE baru selesai saat koneksi putus dan health
	// check hanya menambah noise.
	return middleware.Chain(mux,
		middleware.SecurityHeaders,
		middleware.CORSMiddleware("*"),
		middleware.MaxBody(0), // 0 = default 1 MB
		middleware.OtelMiddleware("queue-service", "/sys/ping", "/stream"),
		middleware.TransactionID,
	), closeFn
}

// runSweeper memanggil sweep berkala sampai ctx dibatalkan.
func runSweeper(ctx context.Context, sweep func(context.Context) error, every time.Duration, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := sweep(ctx); err != nil && ctx.Err() == nil {
				log.Warn("sweeper gagal", slog.Any("err", err))
			}
		}
	}
}
