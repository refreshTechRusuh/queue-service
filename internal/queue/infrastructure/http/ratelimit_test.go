package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

// Redis DB 13 khusus paket test ini (paket lain memakai 14/15; paket berjalan paralel).
func newRateRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 13})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis tidak tersedia: %v", err)
	}
	_ = rdb.FlushDB(context.Background()).Err()
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestJoinRateLimit_ShouldReturn429_WhenLimitExceeded(t *testing.T) {
	rdb := newRateRedis(t)
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	h := JoinRateLimit(rdb, 2)(ok)

	codes := []int{}
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodPost, "/join", nil)
		req.Header.Set("X-Forwarded-For", "203.0.113.7, 10.0.0.1")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		codes = append(codes, rec.Code)
	}
	if codes[0] != 200 || codes[1] != 200 || codes[2] != http.StatusTooManyRequests {
		t.Errorf("kode = %v; mau [200 200 429]", codes)
	}

	// IP lain punya kuota sendiri.
	req := httptest.NewRequest(http.MethodPost, "/join", nil)
	req.Header.Set("X-Forwarded-For", "198.51.100.9")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("IP lain: %d; mau 200", rec.Code)
	}
}

func TestJoinRateLimit_ShouldPassThrough_WhenDisabledOrRedisDown(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	// limit 0 = dimatikan
	rec := httptest.NewRecorder()
	JoinRateLimit(nil, 0)(ok).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/join", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("limiter mati: %d; mau 200", rec.Code)
	}

	// Redis mati → fail-open (journey tidak boleh putus karena limiter)
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer dead.Close()
	rec = httptest.NewRecorder()
	JoinRateLimit(dead, 1)(ok).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/join", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("redis mati: %d; mau 200 (fail-open)", rec.Code)
	}
}

func TestClientIP(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "192.0.2.5:4321"
	if got := clientIP(r); got != "192.0.2.5" {
		t.Errorf("tanpa XFF: %q; mau 192.0.2.5", got)
	}
	r.Header.Set("X-Forwarded-For", " 203.0.113.7 , 10.0.0.1")
	if got := clientIP(r); got != "203.0.113.7" {
		t.Errorf("dengan XFF: %q; mau 203.0.113.7", got)
	}
}
