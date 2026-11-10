package config

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_ShouldUseSafeDefaults_WhenEnvEmpty(t *testing.T) {
	cfg, err := Load()

	require.NoError(t, err)
	assert.Equal(t, 8084, cfg.App.Port)
	assert.Equal(t, 5*time.Minute, cfg.Queue.BookingTTL)
	assert.Equal(t, 1000, cfg.Queue.GlobalBookingCap)
	assert.Equal(t, 100000, cfg.Queue.QueueCap)
	assert.Equal(t, time.Second, cfg.Queue.SweepInterval)
	assert.Equal(t, 2*time.Second, cfg.Queue.SSETick)
	assert.Equal(t, 30, cfg.Queue.JoinRatePerMin)
	assert.True(t, cfg.Kafka.Enabled)
	assert.Equal(t, "queue-service", cfg.Kafka.GroupID)
}

func TestLoad_ShouldReadEnvOverrides(t *testing.T) {
	t.Setenv("APP_PORT", "9000")
	t.Setenv("BOOKING_TTL_SECONDS", "120")
	t.Setenv("QUEUE_SWEEP_INTERVAL_MS", "250")
	t.Setenv("KAFKA_ENABLED", "false")
	t.Setenv("KAFKA_TLS", "yes")
	t.Setenv("REDIS_ADDR", "10.0.0.2:6379")

	cfg, err := Load()

	require.NoError(t, err)
	assert.Equal(t, 9000, cfg.App.Port)
	assert.Equal(t, 2*time.Minute, cfg.Queue.BookingTTL)
	assert.Equal(t, 250*time.Millisecond, cfg.Queue.SweepInterval)
	assert.False(t, cfg.Kafka.Enabled)
	assert.True(t, cfg.Kafka.TLS)
	assert.Equal(t, "10.0.0.2:6379", cfg.Redis.Addr)
}

func TestLoad_ShouldRejectInvalidConfig(t *testing.T) {
	t.Run("TTL booking <= 0", func(t *testing.T) {
		t.Setenv("BOOKING_TTL_SECONDS", "0")
		_, err := Load()
		assert.Error(t, err)
	})
	t.Run("production dengan secret bawaan dev", func(t *testing.T) {
		t.Setenv("APP_ENV", "production")
		_, err := Load()
		assert.Error(t, err)
	})
	t.Run("production dengan secret sendiri", func(t *testing.T) {
		t.Setenv("APP_ENV", "production")
		t.Setenv("ACCESS_TOKEN_SECRET", "rahasia-produksi")
		_, err := Load()
		assert.NoError(t, err)
	})
}

func TestGetEnvBool(t *testing.T) {
	cases := map[string]bool{"true": true, "TRUE": true, "1": true, "yes": true, "false": false, "0": false, "no": false}
	for in, want := range cases {
		t.Setenv("FLAG", in)
		assert.Equal(t, want, getEnvBool("FLAG", !want), "FLAG=%s", in)
	}
	t.Setenv("FLAG", "entah")
	assert.True(t, getEnvBool("FLAG", true), "nilai tak dikenal → fallback")
	assert.False(t, getEnvBool("FLAG", false))
}

func TestGetEnvInt_ShouldFallBack_WhenNotANumber(t *testing.T) {
	t.Setenv("RUSAK", "abc")
	t.Setenv("BAGUS", "42")

	assert.Equal(t, 7, getEnvInt("RUSAK", 7))
	assert.Equal(t, 42, getEnvInt("BAGUS", 7))
	assert.Equal(t, 7, getEnvInt("TIDAK_ADA", 7))
}

func TestDSN(t *testing.T) {
	d := DBConfig{Host: "h", Port: 5432, User: "u", Password: "p", Name: "n", SSLMode: "require"}

	assert.Equal(t, "host=h port=5432 user=u password=p dbname=n sslmode=require connect_timeout=5", d.DSN())
}

func TestSetupLogger_ShouldApplyLevelAndFormat(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	ctx := context.Background()

	for level, minEnabled := range map[string]slog.Level{
		"debug": slog.LevelDebug, "info": slog.LevelInfo, "warn": slog.LevelWarn,
		"error": slog.LevelError, "xxx": slog.LevelInfo,
	} {
		for _, format := range []string{"json", "text"} {
			SetupLogger(LogConfig{Level: level, Format: format})
			for _, l := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
				assert.Equal(t, l >= minEnabled, slog.Default().Enabled(ctx, l), "level=%s format=%s cek=%v", level, format, l)
			}
		}
	}
}
