// Package config memuat konfigurasi queue-service dari environment variable.
// Pola sama dengan service lain (dukcapil-mock, user-service).
package config

import (
	"log/slog"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config = seluruh konfigurasi service, dikelompokkan per bagian.
type Config struct {
	App   AppConfig
	Log   LogConfig
	Redis RedisConfig
}

type AppConfig struct {
	Env  string
	Port int
}

type LogConfig struct {
	Level  string
	Format string
}

// RedisConfig = state antrian (sorted set, counter occupancy, cache stok) ada di Redis.
type RedisConfig struct {
	Addr     string
	Password string `json:"-"`
	DB       int
}

// Load membaca .env (bila ada) lalu environment, dengan default aman untuk dev.
func Load() (*Config, error) {
	_ = godotenv.Load() // .env opsional

	cfg := &Config{
		App: AppConfig{
			Env:  getEnv("APP_ENV", "local"),
			Port: getEnvInt("APP_PORT", 8084), // 8084 = queue-service
		},
		Log: LogConfig{
			Level:  getEnv("LOG_LEVEL", "info"),
			Format: getEnv("LOG_FORMAT", "json"),
		},
		Redis: RedisConfig{
			Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       getEnvInt("REDIS_DB", 0),
		},
	}
	return cfg, nil
}

// SetupLogger memasang slog global sesuai LogConfig (json/text + level).
func SetupLogger(cfg LogConfig) {
	level := slog.LevelInfo
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	var h slog.Handler
	if cfg.Format == "text" {
		h = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	} else {
		h = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	}
	slog.SetDefault(slog.New(h))
}

// --- helper baca env dengan fallback ---

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}
