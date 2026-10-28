// Package db (infrastructure) = sumber stok awal (cold start) dari PostgreSQL.
//
// Kenapa ada: cache stok Redis diisi oleh event Kafka stock.changed, yang hanya
// dikirim saat stok BERUBAH. Event yang baru dibuat belum pernah mengirimnya →
// cache kosong → cap 0 → tak ada yang di-admit. Jadi saat cache kosong, queue
// membaca stok SEKALI (read-only) dari ticket_category, lalu selanjutnya hanya
// mengikuti stock.changed. Queue tidak pernah menulis ke DB.
package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// StockSource = implementasi outbound.IStockSource.
type StockSource struct {
	pool *pgxpool.Pool
}

func NewStockSource(pool *pgxpool.Pool) *StockSource { return &StockSource{pool: pool} }

// LoadStock mengembalikan available per kategori. Map kosong = event tak punya kategori.
func (s *StockSource) LoadStock(ctx context.Context, eventID string) (map[string]int, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id::text, available FROM ticket_category WHERE event_id = $1`, eventID)
	if err != nil {
		return nil, fmt.Errorf("query ticket_category: %w", err)
	}
	defer rows.Close()

	out := make(map[string]int)
	for rows.Next() {
		var id string
		var available int
		if err := rows.Scan(&id, &available); err != nil {
			return nil, fmt.Errorf("scan ticket_category: %w", err)
		}
		out[id] = available
	}
	return out, rows.Err()
}
