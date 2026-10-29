// Package entity berisi entity & aturan murni domain antrian (tanpa Redis/HTTP).
package entity

import "time"

// Status = keadaan sebuah queueToken.
type Status string

const (
	StatusQueued   Status = "QUEUED"   // menunggu di antrian (sorted set)
	StatusAdmitted Status = "ADMITTED" // sudah di booking room, pegang Access Token
	StatusReleased Status = "RELEASED" // kursi dilepas lebih awal (mis. slot.released ber-queueToken)
	StatusExpired  Status = "EXPIRED"  // Access Token kedaluwarsa → kursi dilepas oleh sweeper
	StatusSoldOut  Status = "SOLD_OUT" // antrian ditutup karena tiket habis
)

// JoinOutcome = hasil atomik percobaan masuk antrian.
type JoinOutcome string

const (
	OutcomeAdmitted JoinOutcome = "ADMITTED"
	OutcomeQueued   JoinOutcome = "QUEUED"
	OutcomeSoldOut  JoinOutcome = "SOLD_OUT" // antrian sudah ditutup → 410
	OutcomeFull     JoinOutcome = "FULL"     // queue cap tercapai → 503
)

// MaxTicketsPerBooking = batas tiket dalam satu kode booking. Dasar rumus anti-PHP.
const MaxTicketsPerBooking = 3

// CapByStock menghitung kapasitas booking room (jumlah ORANG) dari stok tersisa.
//
//	stok <= 0 → 0 (tak ada yang di-admit)
//	stok <= 3 → 1 (tiket terakhir dijual satu-orang-per-waktu)
//	selain itu → floor(stok / 3)
//
// Kenapa: tiap orang maksimal 3 tiket. Bila jumlah orang ≤ stok/3, total permintaan
// maksimum ≤ stok, jadi hold saat submit PASTI berhasil → tidak ada yang di-PHP.
// globalCap > 0 = plafon server (GLOBAL_BOOKING_CAP); 0 = tanpa plafon.
//
// CATATAN: rumus yang sama diimplementasikan ulang di Lua (infrastructure/cache)
// supaya dihitung atomik di Redis. Keduanya wajib identik — dijaga oleh test.
func CapByStock(stock, globalCap int) int {
	var c int
	switch {
	case stock <= 0:
		c = 0
	case stock <= MaxTicketsPerBooking:
		c = 1
	default:
		c = stock / MaxTicketsPerBooking
	}
	if globalCap > 0 && c > globalCap {
		c = globalCap
	}
	return c
}

// AccessClaims = isi Access Token (JWT) yang diterbitkan saat ADMITTED.
// Bentuknya HARUS cocok dengan yang divalidasi booking/event/payment-service.
type AccessClaims struct {
	EventID    string
	QueueToken string
	IssuedAt   time.Time
	ExpiresAt  time.Time // = batas TTL booking (5 menit sejak admitted)
}
