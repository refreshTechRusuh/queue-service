// Integration test terhadap PostgreSQL sungguhan. Di-skip bila DB tidak tersedia.
// Baris uji memakai UUID acak dan dihapus kembali di akhir test.
package db

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		dsn = "host=localhost port=5432 user=postgres password=password dbname=rusuh_booking sslmode=disable connect_timeout=3"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err == nil {
		err = pool.Ping(ctx)
	}
	if err != nil {
		t.Skipf("postgres tidak tersedia: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestLoadStock_ShouldReturnAvailablePerCategory(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	eventID, catA, catB := uuid.NewString(), uuid.NewString(), uuid.NewString()

	if _, err := pool.Exec(ctx, `INSERT INTO event (id, name, status) VALUES ($1, 'test stock source', 'on_sale')`, eventID); err != nil {
		t.Skipf("tabel event tidak tersedia: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM ticket_category WHERE event_id = $1`, eventID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM event WHERE id = $1`, eventID)
	})
	if _, err := pool.Exec(ctx,
		`INSERT INTO ticket_category (id, event_id, name, price, capacity, available)
		 VALUES ($1, $3, 'VIP', 100, 10, 7), ($2, $3, 'Reguler', 50, 20, 0)`, catA, catB, eventID); err != nil {
		t.Fatalf("insert kategori: %v", err)
	}

	got, err := NewStockSource(pool).LoadStock(ctx, eventID)
	if err != nil {
		t.Fatalf("LoadStock: %v", err)
	}
	if len(got) != 2 || got[catA] != 7 || got[catB] != 0 {
		t.Errorf("LoadStock = %v; mau {%s:7, %s:0}", got, catA, catB)
	}

	empty, err := NewStockSource(pool).LoadStock(ctx, uuid.NewString())
	if err != nil || len(empty) != 0 {
		t.Errorf("event tak dikenal: %v err=%v; mau map kosong", empty, err)
	}
}
