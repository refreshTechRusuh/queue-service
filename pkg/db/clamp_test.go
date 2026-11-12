package db

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToInt32_ShouldClampIntoInt32Range(t *testing.T) {
	cases := map[string]struct {
		in   int
		want int32
	}{
		"nol":              {0, 0},
		"nilai normal":     {10, 10},
		"batas atas pas":   {math.MaxInt32, math.MaxInt32},
		"melewati batas":   {math.MaxInt32 + 1, math.MaxInt32},
		"sangat besar":     {math.MaxInt64, math.MaxInt32},
		"negatif jadi nol": {-5, 0},
		"sangat negatif":   {math.MinInt64, 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, toInt32(tc.in))
		})
	}
}
