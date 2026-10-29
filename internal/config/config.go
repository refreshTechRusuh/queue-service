// Package config memuat konfigurasi queue-service dari environment variable.
// Pola sama dengan service lain (dukcapil-mock, user-service).
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Config = seluruh konfigurasi service, dikelompokkan per bagian.
type Config struct {
	App   AppConfig
	Log   LogConfig
	Redis RedisConfig
	DB    DBConfig
	JWT   JWTConfig
	Queue QueueConfig
	Kafka KafkaConfig
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

// DBConfig = PostgreSQL bersama, dipakai HANYA untuk warm-up stok (read-only).
type DBConfig struct {
	Host     string
	Port     int
	User     string
	Password string `json:"-"`
	Name     string
	SSLMode  string
	MaxConns int
}

// JWTConfig: secret Access Token HARUS sama dengan booking/event/payment-service.
type JWTConfig struct {
	AccessTokenSecret string `json:"-"`
}

// QueueConfig = parameter kapasitas & waktu (HLD §11).
type QueueConfig struct {
	BookingTTL       time.Duration // umur kursi/Access Token (5 menit, BR-03)
	GlobalBookingCap int           // plafon orang di booking room per event (0 = tanpa plafon)
	QueueCap         int           // panjang antrian maksimum (Req 22; 0 = tanpa batas)
	StateTTL         time.Duration // umur data token di Redis
	ClosedTTL        time.Duration // umur flag sold-out
	SweepInterval    time.Duration // interval sweeper kursi kedaluwarsa
	SSETick          time.Duration // interval cek ulang + heartbeat SSE
	JoinRatePerMin   int           // batas join per IP per menit (0 = mati)
}

// KafkaConfig = consumer event BE-2. Lokal: broker docker-compose (host port 29092).
type KafkaConfig struct {
	Enabled  bool
	Brokers  string
	Username string
	Password string `json:"-"`
	TLS      bool
	GroupID  string
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
		DB: DBConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     getEnvInt("DB_PORT", 5432),
			User:     getEnv("DB_USER", "postgres"),
			Password: getEnv("DB_PASSWORD", "password"),
			Name:     getEnv("DB_NAME", "rusuh_booking"),
			SSLMode:  getEnv("DB_SSL_MODE", "disable"),
			MaxConns: getEnvInt("DB_MAX_CONNS", 5),
		},
		JWT: JWTConfig{
			AccessTokenSecret: getEnv("ACCESS_TOKEN_SECRET", "dev-secret-change-in-production"),
		},
		Queue: QueueConfig{
			BookingTTL:       seconds("BOOKING_TTL_SECONDS", 300),
			GlobalBookingCap: getEnvInt("GLOBAL_BOOKING_CAP", 1000),
			QueueCap:         getEnvInt("QUEUE_CAP", 100000),
			StateTTL:         seconds("QUEUE_STATE_TTL_SECONDS", 6*3600),
			ClosedTTL:        seconds("QUEUE_CLOSED_TTL_SECONDS", 7*24*3600),
			SweepInterval:    millis("QUEUE_SWEEP_INTERVAL_MS", 1000),
			SSETick:          seconds("QUEUE_SSE_TICK_SECONDS", 2),
			JoinRatePerMin:   getEnvInt("QUEUE_JOIN_RATE_PER_MIN", 30),
		},
		Kafka: KafkaConfig{
			Enabled:  getEnvBool("KAFKA_ENABLED", true),
			Brokers:  getEnv("KAFKA_BROKERS", "localhost:29092"),
			Username: getEnv("KAFKA_USERNAME", ""),
			Password: getEnv("KAFKA_PASSWORD", ""),
			TLS:      getEnvBool("KAFKA_TLS", false),
			GroupID:  getEnv("KAFKA_GROUP_ID", "queue-service"),
		},
	}

	if cfg.Queue.BookingTTL <= 0 {
		return nil, fmt.Errorf("config: BOOKING_TTL_SECONDS harus > 0")
	}
	if cfg.App.Env == "production" && cfg.JWT.AccessTokenSecret == "dev-secret-change-in-production" {
		return nil, fmt.Errorf("config: ACCESS_TOKEN_SECRET wajib di-set di production")
	}
	return cfg, nil
}

// DSN membentuk connection string PostgreSQL.
func (d DBConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=5",
		d.Host, d.Port, d.User, d.Password, d.Name, d.SSLMode)
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

func getEnvBool(key string, fallback bool) bool {
	switch strings.ToLower(os.Getenv(key)) {
	case "true", "1", "yes":
		return true
	case "false", "0", "no":
		return false
	default:
		return fallback
	}
}

func seconds(key string, fallback int) time.Duration {
	return time.Duration(getEnvInt(key, fallback)) * time.Second
}

func millis(key string, fallback int) time.Duration {
	return time.Duration(getEnvInt(key, fallback)) * time.Millisecond
}
