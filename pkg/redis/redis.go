// Package redis menyediakan koneksi Redis (go-redis) + Ping saat startup.
package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/refreshTechRusuh/queue-service/internal/config"
)

// NewClient membuat client Redis dari RedisConfig lalu memastikan hidup via Ping.
func NewClient(ctx context.Context, cfg config.RedisConfig) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		return nil, fmt.Errorf("redis: ping: %w", err)
	}
	return rdb, nil
}
