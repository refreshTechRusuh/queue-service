// Package main = entry point queue-service.
// Tugas main: startup (load config, konek Redis, rakit server) + graceful shutdown.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/refreshTechRusuh/queue-service/internal/config"
	"github.com/refreshTechRusuh/queue-service/internal/delivery"
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

	rootCtx, cancelRoot := context.WithCancel(context.Background())
	defer cancelRoot()

	// 2. Redis = sumber state antrian. Gagal konek = gagal start (fail fast).
	rdb, err := pkgredis.NewClient(rootCtx, cfg.Redis)
	if err != nil {
		slog.Error("gagal konek redis", "err", err)
		os.Exit(1)
	}
	defer func() { _ = rdb.Close() }()

	// 3. Rakit server.
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.App.Port),
		Handler:           delivery.New(cfg, rdb),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// 4. Jalankan di goroutine supaya main bisa menunggu sinyal berhenti.
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
	cancelRoot()
	shutCtx, cancelShut := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShut()
	if err := srv.Shutdown(shutCtx); err != nil {
		slog.Error("shutdown error", "err", err)
	}
	slog.Info("queue-service stopped")
}
