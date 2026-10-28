package cache

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cloud Run menjalankan banyak instance. Promosi bisa terjadi di instance A
// (yang membaca Kafka), sementara koneksi SSE user ada di instance B.
// Redis Pub/Sub menyampaikan sinyal "state berubah" ke SEMUA instance; tiap
// instance lalu membangunkan koneksi SSE lokal yang relevan.
//
// Channel: queue:notify:<eventId>  payload: "tok1,tok2,..." atau "*" (semua).

const notifyPrefix = "queue:notify:"

// Notifier mempublikasikan sinyal (implementasi outbound.INotifier).
type Notifier struct {
	rdb *redis.Client
}

func NewNotifier(rdb *redis.Client) *Notifier { return &Notifier{rdb: rdb} }

func (n *Notifier) Notify(ctx context.Context, eventID string, tokens []string) error {
	if len(tokens) == 0 {
		return nil
	}
	return n.rdb.Publish(ctx, notifyPrefix+eventID, strings.Join(tokens, ",")).Err()
}

func (n *Notifier) NotifyAll(ctx context.Context, eventID string) error {
	return n.rdb.Publish(ctx, notifyPrefix+eventID, "*").Err()
}

// Hub menerima sinyal Pub/Sub dan membangunkan koneksi SSE lokal.
type Hub struct {
	rdb  *redis.Client
	mu   sync.Mutex
	subs map[string]map[string]map[chan struct{}]struct{} // eventId → token → set channel
}

func NewHub(rdb *redis.Client) *Hub {
	return &Hub{rdb: rdb, subs: make(map[string]map[string]map[chan struct{}]struct{})}
}

// Subscribe mendaftarkan satu koneksi SSE. Channel ber-buffer 1: sinyal yang
// datang beruntun cukup dilebur jadi satu "bangun" (handler membaca state terbaru).
func (h *Hub) Subscribe(eventID, token string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	if h.subs[eventID] == nil {
		h.subs[eventID] = make(map[string]map[chan struct{}]struct{})
	}
	if h.subs[eventID][token] == nil {
		h.subs[eventID][token] = make(map[chan struct{}]struct{})
	}
	h.subs[eventID][token][ch] = struct{}{}
	h.mu.Unlock()

	unsubscribe := func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.subs[eventID][token], ch)
		if len(h.subs[eventID][token]) == 0 {
			delete(h.subs[eventID], token)
		}
		if len(h.subs[eventID]) == 0 {
			delete(h.subs, eventID)
		}
	}
	return ch, unsubscribe
}

// Run berlangganan Pub/Sub sampai ctx dibatalkan. Reconnect otomatis bila putus.
func (h *Hub) Run(ctx context.Context) {
	for ctx.Err() == nil {
		ps := h.rdb.PSubscribe(ctx, notifyPrefix+"*")
		for msg := range ps.Channel() {
			h.dispatch(strings.TrimPrefix(msg.Channel, notifyPrefix), msg.Payload)
		}
		_ = ps.Close()
		if ctx.Err() == nil {
			slog.Warn("queue hub: pubsub terputus, reconnect")
			time.Sleep(time.Second)
		}
	}
}

func (h *Hub) dispatch(eventID, payload string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	byToken := h.subs[eventID]
	if len(byToken) == 0 {
		return
	}
	if payload == "*" {
		for _, chans := range byToken {
			wake(chans)
		}
		return
	}
	for _, tok := range strings.Split(payload, ",") {
		wake(byToken[tok])
	}
}

func wake(chans map[chan struct{}]struct{}) {
	for ch := range chans {
		select {
		case ch <- struct{}{}:
		default: // sudah ada sinyal menunggu → cukup satu
		}
	}
}
