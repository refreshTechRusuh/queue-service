// Test end-to-end adapter HTTP + SSE terhadap Redis sungguhan (DB 14).
// DB berbeda dari test usecase (15) karena `go test ./...` menjalankan paket
// secara paralel dan tiap paket mengosongkan DB-nya sendiri.
package http_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/refreshTechRusuh/queue-service/internal/queue/application/usecase"
	"github.com/refreshTechRusuh/queue-service/internal/queue/infrastructure/cache"
	queuehttp "github.com/refreshTechRusuh/queue-service/internal/queue/infrastructure/http"
	"github.com/refreshTechRusuh/queue-service/internal/queue/infrastructure/token"
)

type staticStock map[string]map[string]int

func (s staticStock) LoadStock(_ context.Context, eventID string) (map[string]int, error) {
	return s[eventID], nil
}

type envelope struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Data    struct {
		Status      string `json:"status"`
		QueueToken  string `json:"queueToken"`
		Position    int64  `json:"position"`
		AccessToken string `json:"accessToken"`
	} `json:"data"`
}

func newServer(t *testing.T, eventID string, stock int) (*httptest.Server, *usecase.QueueUsecase) {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 14})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis tidak tersedia: %v", err)
	}
	_ = rdb.FlushDB(context.Background()).Err()

	store := cache.NewQueueStore(rdb, cache.StoreSettings{
		BookingTTL: 5 * time.Minute, StateTTL: time.Hour, ClosedTTL: time.Hour,
	})
	hub := cache.NewHub(rdb)
	ctx, cancel := context.WithCancel(context.Background())
	go hub.Run(ctx)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	uc := usecase.NewQueueUsecase(store, staticStock{eventID: {"cat-1": stock}},
		cache.NewNotifier(rdb), token.NewSigner("test-secret"),
		usecase.Settings{BookingTTL: 5 * time.Minute}, log)

	// sseTick sengaja 30 detik: kalau event `admitted` datang cepat, itu
	// membuktikan stream dibangunkan Pub/Sub, bukan oleh polling tick.
	h := queuehttp.NewHandler(uc, hub, 30*time.Second)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/queue/v1/event/{eventId}/join", h.Join)
	mux.HandleFunc("GET /api/queue/v1/event/{eventId}/{queueToken}", h.Status)
	mux.HandleFunc("GET /api/queue/v1/event/{eventId}/{queueToken}/stream", h.Stream)
	srv := httptest.NewServer(mux)

	t.Cleanup(func() {
		srv.Close()
		cancel()
		_ = rdb.Close()
	})
	time.Sleep(100 * time.Millisecond) // beri waktu hub berlangganan
	return srv, uc
}

func join(t *testing.T, srv *httptest.Server, eventID string) envelope {
	t.Helper()
	resp, err := http.Post(srv.URL+"/api/queue/v1/event/"+eventID+"/join", "application/json", nil)
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	defer resp.Body.Close()
	var env envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode join: %v", err)
	}
	return env
}

func TestStream_ShouldPushAdmitted_WhenSeatFreedViaPubSub(t *testing.T) {
	eventID := uuid.NewString()
	srv, uc := newServer(t, eventID, 3) // cap 1

	a := join(t, srv, eventID)
	b := join(t, srv, eventID)
	if a.Data.Status != "ADMITTED" || b.Data.Status != "QUEUED" {
		t.Fatalf("awal: a=%s b=%s; mau ADMITTED, QUEUED", a.Data.Status, b.Data.Status)
	}

	resp, err := http.Get(srv.URL + "/api/queue/v1/event/" + eventID + "/" + b.Data.QueueToken + "/stream")
	if err != nil {
		t.Fatalf("buka stream: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q; mau text/event-stream", ct)
	}

	events := make(chan [2]string, 10)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		var name string
		for sc.Scan() {
			line := sc.Text()
			switch {
			case strings.HasPrefix(line, "event: "):
				name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				events <- [2]string{name, strings.TrimPrefix(line, "data: ")}
			}
		}
		close(events)
	}()

	// 1) event pertama = posisi 1
	select {
	case ev := <-events:
		if ev[0] != "position" || !strings.Contains(ev[1], `"position":1`) {
			t.Fatalf("event pertama = %v; mau position 1", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("tidak ada event position")
	}

	// 2) kursi A dilepas → B harus menerima `admitted` jauh sebelum tick 30 detik
	if err := uc.OnSlotReleased(context.Background(), eventID, a.Data.QueueToken); err != nil {
		t.Fatalf("release: %v", err)
	}
	select {
	case ev := <-events:
		if ev[0] != "admitted" || !strings.Contains(ev[1], `"accessToken":"`) {
			t.Fatalf("event kedua = %v; mau admitted dengan accessToken", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("event admitted tidak terkirim lewat Pub/Sub dalam 3 detik")
	}
}

func TestHTTP_ShouldMapErrorsToStatusCodes(t *testing.T) {
	eventID := uuid.NewString()
	srv, uc := newServer(t, eventID, 0)

	cases := []struct {
		name string
		do   func() (*http.Response, error)
		want int
	}{
		{"eventId tidak valid → 400", func() (*http.Response, error) {
			return http.Post(srv.URL+"/api/queue/v1/event/abc/join", "application/json", nil)
		}, http.StatusBadRequest},
		{"event tidak ada → 404", func() (*http.Response, error) {
			return http.Post(srv.URL+"/api/queue/v1/event/"+uuid.NewString()+"/join", "application/json", nil)
		}, http.StatusNotFound},
		{"token tak dikenal → 404", func() (*http.Response, error) {
			return http.Get(srv.URL + "/api/queue/v1/event/" + eventID + "/q_" + uuid.NewString())
		}, http.StatusNotFound},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp, err := c.do()
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != c.want {
				t.Errorf("HTTP %d; mau %d", resp.StatusCode, c.want)
			}
		})
	}

	// sold out → join 410
	if err := uc.OnSoldOut(context.Background(), eventID); err != nil {
		t.Fatalf("sold out: %v", err)
	}
	resp, err := http.Post(srv.URL+"/api/queue/v1/event/"+eventID+"/join", "application/json", nil)
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusGone {
		t.Errorf("join setelah sold out: HTTP %d; mau 410", resp.StatusCode)
	}
}
