// Package main = entry point queue-service.
// Tugas main: startup (config, Redis, DB, rakit server) + graceful shutdown.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/refreshTechRusuh/queue-service/internal/config"
	"github.com/refreshTechRusuh/queue-service/internal/delivery"
	pkgdb "github.com/refreshTechRusuh/queue-service/pkg/db"
	pkgredis "github.com/refreshTechRusuh/queue-service/pkg/redis"
)

func main() {
	// 1. Konfigurasi dari env (+ .env bila ada).
	cfg, err := config.Load()
	if err != nil {
		slog.Error("gagal load config", "err", err)
		os.Exit(1)
	}
	config.SetupLogger(cfg.Log)

	// rootCtx dibatalkan saat shutdown → menghentikan hub, sweeper, consumer Kafka.
	rootCtx, cancelRoot := context.WithCancel(context.Background())
	defer cancelRoot()

	// 2. Redis = sumber state antrian. Gagal konek = gagal start (fail fast).
	rdb, err := pkgredis.NewClient(rootCtx, cfg.Redis)
	if err != nil {
		slog.Error("gagal konek redis", "err", err)
		os.Exit(1)
	}
	defer func() { _ = rdb.Close() }()

	// 3. PostgreSQL hanya untuk warm-up stok (read-only). Belum siap ≠ gagal start:
	//    event yang cache stoknya sudah ada tetap bisa dilayani.
	pool, err := pkgdb.NewPool(rootCtx, pkgdb.Config{DSN: cfg.DB.DSN(), MaxConns: cfg.DB.MaxConns})
	if err != nil {
		slog.Error("konfigurasi postgres tidak valid", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	pingCtx, cancelPing := context.WithTimeout(rootCtx, 5*time.Second)
	if err := pool.Ping(pingCtx); err != nil {
		slog.Warn("postgres belum terjangkau; warm-up stok akan gagal sampai DB siap", "err", err)
	}
	cancelPing()

	// 4. Rakit server + proses latar.
	handler, closeDeps := delivery.New(rootCtx, cfg, rdb, pool)
	defer closeDeps()

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.App.Port),
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		// Tanpa WriteTimeout: koneksi SSE memang dibiarkan terbuka lama.
		// Context tiap request diturunkan dari rootCtx → cancelRoot() memutus semua
		// stream SSE, sehingga Shutdown tidak menunggu koneksi yang tak pernah selesai.
		BaseContext: func(net.Listener) context.Context { return rootCtx },
	}

	go func() {
		slog.Info("queue-service listening", "port", cfg.App.Port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	// 5. Tunggu Ctrl+C / SIGTERM, lalu matikan rapi.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down...")
	cancelRoot() // hentikan proses latar + putus koneksi SSE (ctx request turunan)
	shutCtx, cancelShut := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShut()
	if err := srv.Shutdown(shutCtx); err != nil {
		slog.Error("shutdown error", "err", err)
	}
	slog.Info("queue-service stopped")
}
