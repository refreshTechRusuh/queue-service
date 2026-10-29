package entity

import "testing"

func TestCapByStock(t *testing.T) {
	cases := []struct {
		name      string
		stock     int
		globalCap int
		want      int
	}{
		{"stok habis → 0", 0, 0, 0},
		{"stok negatif (anomali) → 0", -5, 0, 0},
		{"stok 1 → 1 orang", 1, 0, 1},
		{"stok 3 → 1 orang", 3, 0, 1},
		{"stok 4 → floor(4/3)=1", 4, 0, 1},
		{"stok 6 → 2", 6, 0, 2},
		{"stok 1000 → 333", 1000, 0, 333},
		{"plafon server membatasi", 1000, 100, 100},
		{"plafon lebih besar tak berpengaruh", 30, 500, 10},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := CapByStock(c.stock, c.globalCap); got != c.want {
				t.Errorf("CapByStock(%d, %d) = %d; mau %d", c.stock, c.globalCap, got, c.want)
			}
		})
	}
}

// TestCapByStock_NeverAllowsOverDemand membuktikan sifat anti-PHP:
// orang × 3 tiket tidak pernah melebihi stok (kecuali kasus stok ≤ 3 → 1 orang).
func TestCapByStock_NeverAllowsOverDemand(t *testing.T) {
	for stock := 4; stock <= 5000; stock++ {
		if people := CapByStock(stock, 0); people*MaxTicketsPerBooking > stock {
			t.Fatalf("stok %d: %d orang × 3 = %d > stok (PHP!)", stock, people, people*3)
		}
	}
}
