// Package db menyediakan connection pool PostgreSQL (pgxpool).
// Di queue-service hanya dipakai untuk warm-up stok (read-only) saat cache kosong.
package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Config = koneksi PostgreSQL.
type Config struct {
	DSN      string `json:"-"`
	MaxConns int
}

// NewPool membuat pool. pgxpool belum benar-benar konek di sini; Ping dilakukan
// terpisah oleh pemanggil supaya DB yang belum siap tidak menggagalkan startup.
func NewPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("db: parse config: %w", err)
	}
	poolCfg.MaxConns = toInt32(cfg.MaxConns)
	poolCfg.MaxConnIdleTime = 5 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("db: connect: %w", err)
	}
	return pool, nil
}
