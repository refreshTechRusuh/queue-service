package db

import "math"

// toInt32 membatasi n ke rentang int32 [0, MaxInt32].
//
// Ukuran pool berasal dari konfigurasi (int). Konversi langsung int -> int32 bisa meluap
// diam-diam bila nilainya salah ketik (mis. 3000000000) dan menghasilkan angka negatif
// yang aneh. Dibatasi di sini; nilai di bawah 1 tetap ditolak pgxpool (fail-fast saat start).
func toInt32(n int) int32 {
	if n < 0 {
		return 0
	}
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(n)
}
