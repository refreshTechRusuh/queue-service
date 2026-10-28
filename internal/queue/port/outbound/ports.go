// Package outbound berisi driven port: kontrak yang DIBUTUHKAN usecase antrian.
package outbound

import (
	"context"
	"time"

	"github.com/refreshTechRusuh/queue-service/internal/queue/domain/entity"
)

// JoinParams = input percobaan masuk antrian.
type JoinParams struct {
	EventID string
	Token   string
	NIK     string // opsional
}

// JoinResult = hasil atomik masuk antrian.
type JoinResult struct {
	Outcome   entity.JoinOutcome
	Position  int64     // saat QUEUED
	ExpiresAt time.Time // saat ADMITTED
	Promoted  []string  // token LAIN yang ikut ter-admit dalam operasi yang sama (perlu dinotifikasi)
}

// TokenState = keadaan satu queueToken.
type TokenState struct {
	Found     bool
	Status    entity.Status
	NIK       string
	ExpiresAt time.Time // saat ADMITTED
	Position  int64     // saat QUEUED
}

// IQueueStore = penyimpanan antrian. SEMUA operasi yang mengubah occupancy WAJIB atomik
// (implementasi Redis memakai Lua script) agar occupancy tak pernah melebihi cap.
type IQueueStore interface {
	// HasStock true bila cache stok event sudah terisi.
	HasStock(ctx context.Context, eventID string) (bool, error)
	// WarmStock mengisi cache stok per kategori TANPA menimpa nilai yang lebih baru.
	WarmStock(ctx context.Context, eventID string, available map[string]int) error
	// SetStock menyimpan available satu kategori (last-write-wins dari stock.changed).
	SetStock(ctx context.Context, eventID, categoryID string, available int) error

	// Join: cek tutup/duplikat/penuh → masuk antrian → promosi FCFS. Atomik.
	Join(ctx context.Context, p JoinParams) (JoinResult, error)
	// State membaca keadaan token (read-only).
	State(ctx context.Context, eventID, token string) (TokenState, error)
	// Promote mengisi slot kosong dari depan antrian (gelombang). Mengembalikan token yang ter-admit.
	Promote(ctx context.Context, eventID string) ([]string, error)
	// Release melepas kursi satu token ADMITTED (idempoten) lalu promosi.
	Release(ctx context.Context, eventID, token string) (released bool, promoted []string, err error)
	// SweepExpired melepas kursi token ADMITTED yang Access Token-nya kedaluwarsa, lalu promosi.
	SweepExpired(ctx context.Context, eventID string) (promoted []string, err error)
	// Close menutup antrian (sold out). Mengembalikan jumlah token antrian yang dikeluarkan.
	Close(ctx context.Context, eventID string) (int, error)

	// RegisterEvent menandai event punya antrian aktif (dipakai sweeper).
	RegisterEvent(ctx context.Context, eventID string) error
	// ActiveEvents = daftar event yang punya antrian (untuk sweeper).
	ActiveEvents(ctx context.Context) ([]string, error)
	// AdmissionRatePerSec = laju admit rata-rata 60 detik terakhir (untuk ETA).
	AdmissionRatePerSec(ctx context.Context, eventID string) (float64, error)
}

// IStockSource = sumber stok awal (cold start) saat cache Redis masih kosong.
// Map kosong (tanpa error) = event tidak punya kategori → dianggap tidak ditemukan.
type IStockSource interface {
	LoadStock(ctx context.Context, eventID string) (map[string]int, error)
}

// INotifier membangunkan koneksi SSE yang relevan (lintas instance) saat state berubah.
type INotifier interface {
	Notify(ctx context.Context, eventID string, tokens []string) error
	NotifyAll(ctx context.Context, eventID string) error
}

// IAccessTokenSigner menerbitkan Access Token (JWT) dari claims.
type IAccessTokenSigner interface {
	Sign(c entity.AccessClaims) (string, error)
}
