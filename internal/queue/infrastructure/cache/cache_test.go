package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/refreshTechRusuh/queue-service/internal/queue/domain/entity"
	"github.com/refreshTechRusuh/queue-service/internal/queue/port/outbound"
)

const (
	ev1  = "0190a8f0-1111-7000-8000-000000000001"
	tokA = "q_0190a8f0-aaaa-7000-8000-00000000000a"
	tokB = "q_0190a8f0-bbbb-7000-8000-00000000000b"
	tokC = "q_0190a8f0-cccc-7000-8000-00000000000c"
)

func newRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb, mr
}

func newStore(t *testing.T, cfg StoreSettings) (*QueueStore, *miniredis.Miniredis) {
	t.Helper()
	rdb, mr := newRedis(t)
	if cfg.BookingTTL == 0 {
		cfg.BookingTTL = 5 * time.Minute
	}
	if cfg.StateTTL == 0 {
		cfg.StateTTL = time.Hour
	}
	if cfg.ClosedTTL == 0 {
		cfg.ClosedTTL = time.Hour
	}
	return NewQueueStore(rdb, cfg), mr
}

func join(t *testing.T, s *QueueStore, token string) outbound.JoinResult {
	t.Helper()
	res, err := s.Join(context.Background(), outbound.JoinParams{EventID: ev1, Token: token})
	require.NoError(t, err)
	return res
}

// =========================================================== QueueStore (perilaku)

func TestStore_Stock(t *testing.T) {
	s, _ := newStore(t, StoreSettings{})
	ctx := context.Background()

	has, err := s.HasStock(ctx, ev1)
	require.NoError(t, err)
	assert.False(t, has)

	require.NoError(t, s.WarmStock(ctx, ev1, map[string]int{"vip": 10, "reg": 20}))
	has, err = s.HasStock(ctx, ev1)
	require.NoError(t, err)
	assert.True(t, has)

	// WarmStock tidak menimpa nilai yang lebih baru dari stock.changed.
	require.NoError(t, s.SetStock(ctx, ev1, "vip", 3))
	require.NoError(t, s.WarmStock(ctx, ev1, map[string]int{"vip": 999}))
	assert.Equal(t, "3", hget(t, s, keyStock(ev1), "vip"))
	assert.Equal(t, "20", hget(t, s, keyStock(ev1), "reg"))
}

func hget(t *testing.T, s *QueueStore, key, field string) string {
	t.Helper()
	v, err := s.rdb.HGet(context.Background(), key, field).Result()
	require.NoError(t, err)
	return v
}

func TestStore_Join_ShouldAdmitFirst_QueueRest_AndReportPosition(t *testing.T) {
	s, _ := newStore(t, StoreSettings{})
	require.NoError(t, s.WarmStock(context.Background(), ev1, map[string]int{"cat": 3})) // cap 1

	a := join(t, s, tokA)
	b := join(t, s, tokB)
	c := join(t, s, tokC)

	assert.Equal(t, entity.OutcomeAdmitted, a.Outcome)
	assert.WithinDuration(t, time.Now().Add(5*time.Minute), a.ExpiresAt, 5*time.Second)
	assert.Equal(t, entity.OutcomeQueued, b.Outcome)
	assert.Equal(t, int64(1), b.Position)
	assert.Equal(t, int64(2), c.Position, "FCFS: yang datang belakangan di belakang")
}

func TestStore_Join_ShouldRejectWhenClosedOrFull(t *testing.T) {
	t.Run("antrian ditutup (sold out)", func(t *testing.T) {
		s, _ := newStore(t, StoreSettings{})
		require.NoError(t, s.WarmStock(context.Background(), ev1, map[string]int{"cat": 9}))
		_, err := s.Close(context.Background(), ev1)
		require.NoError(t, err)

		assert.Equal(t, entity.OutcomeSoldOut, join(t, s, tokA).Outcome)
	})
	t.Run("queue cap tercapai", func(t *testing.T) {
		s, _ := newStore(t, StoreSettings{QueueCap: 1})
		require.NoError(t, s.WarmStock(context.Background(), ev1, map[string]int{"cat": 0})) // cap room 0 → semua mengantre

		assert.Equal(t, entity.OutcomeQueued, join(t, s, tokA).Outcome)
		assert.Equal(t, entity.OutcomeFull, join(t, s, tokB).Outcome)
	})
}

func TestStore_State(t *testing.T) {
	s, _ := newStore(t, StoreSettings{})
	ctx := context.Background()
	require.NoError(t, s.WarmStock(ctx, ev1, map[string]int{"cat": 3}))
	join(t, s, tokA)
	join(t, s, tokB)

	t.Run("token tak dikenal", func(t *testing.T) {
		st, err := s.State(ctx, ev1, "q_tidak-ada")
		require.NoError(t, err)
		assert.False(t, st.Found)
	})
	t.Run("ADMITTED membawa waktu kedaluwarsa", func(t *testing.T) {
		st, err := s.State(ctx, ev1, tokA)
		require.NoError(t, err)
		assert.True(t, st.Found)
		assert.Equal(t, entity.StatusAdmitted, st.Status)
		assert.False(t, st.ExpiresAt.IsZero())
	})
	t.Run("QUEUED membawa posisi (1 = terdepan)", func(t *testing.T) {
		st, err := s.State(ctx, ev1, tokB)
		require.NoError(t, err)
		assert.Equal(t, entity.StatusQueued, st.Status)
		assert.Equal(t, int64(1), st.Position)
	})
}

func TestStore_Release_ShouldBeIdempotent_AndPromoteNext(t *testing.T) {
	s, _ := newStore(t, StoreSettings{})
	ctx := context.Background()
	require.NoError(t, s.WarmStock(ctx, ev1, map[string]int{"cat": 3})) // cap 1
	join(t, s, tokA)
	join(t, s, tokB)

	released, promoted, err := s.Release(ctx, ev1, tokA)
	require.NoError(t, err)
	assert.True(t, released)
	assert.Equal(t, []string{tokB}, promoted, "kursi kosong langsung diisi orang terdepan")

	again, promotedAgain, err := s.Release(ctx, ev1, tokA)
	require.NoError(t, err)
	assert.False(t, again, "rilis kedua tidak boleh melepas kursi orang lain")
	assert.Empty(t, promotedAgain)
	assert.Equal(t, "1", mustGet(t, s, keyOccupancy(ev1)), "occupancy tidak boleh turun di bawah kenyataan")
}

func mustGet(t *testing.T, s *QueueStore, key string) string {
	t.Helper()
	v, err := s.rdb.Get(context.Background(), key).Result()
	require.NoError(t, err)
	return v
}

func TestStore_Promote_ShouldFillFreeSeats_WhenStockGrows(t *testing.T) {
	s, _ := newStore(t, StoreSettings{})
	ctx := context.Background()
	require.NoError(t, s.WarmStock(ctx, ev1, map[string]int{"cat": 3})) // cap 1
	join(t, s, tokA)
	join(t, s, tokB)
	join(t, s, tokC)

	require.NoError(t, s.SetStock(ctx, ev1, "cat", 9)) // cap 3
	promoted, err := s.Promote(ctx, ev1)

	require.NoError(t, err)
	assert.Equal(t, []string{tokB, tokC}, promoted, "gelombang FCFS")
}

func TestStore_SweepExpired_ShouldReleaseOnlyExpiredSeats(t *testing.T) {
	s, _ := newStore(t, StoreSettings{BookingTTL: 50 * time.Millisecond})
	ctx := context.Background()
	require.NoError(t, s.WarmStock(ctx, ev1, map[string]int{"cat": 3})) // cap 1
	join(t, s, tokA)
	join(t, s, tokB)

	early, err := s.SweepExpired(ctx, ev1)
	require.NoError(t, err)
	assert.Empty(t, early, "kursi belum kedaluwarsa tidak boleh disapu")

	time.Sleep(120 * time.Millisecond)
	promoted, err := s.SweepExpired(ctx, ev1)

	require.NoError(t, err)
	assert.Equal(t, []string{tokB}, promoted)
}

func TestStore_Close_ShouldCountQueuedAndMarkThemSoldOut(t *testing.T) {
	s, _ := newStore(t, StoreSettings{})
	ctx := context.Background()
	require.NoError(t, s.WarmStock(ctx, ev1, map[string]int{"cat": 3}))
	join(t, s, tokA)
	join(t, s, tokB)
	join(t, s, tokC)

	n, err := s.Close(ctx, ev1)

	require.NoError(t, err)
	assert.Equal(t, 2, n, "hanya yang menunggu yang dikeluarkan; yang sudah di booking room tetap")
	st, err := s.State(ctx, ev1, tokB)
	require.NoError(t, err)
	assert.Equal(t, entity.StatusSoldOut, st.Status)
}

func TestStore_ActiveEventsAndAdmissionRate(t *testing.T) {
	s, _ := newStore(t, StoreSettings{})
	ctx := context.Background()

	require.NoError(t, s.RegisterEvent(ctx, ev1))
	require.NoError(t, s.RegisterEvent(ctx, ev1)) // idempoten (SET)
	ids, err := s.ActiveEvents(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{ev1}, ids)

	rate0, err := s.AdmissionRatePerSec(ctx, ev1)
	require.NoError(t, err)
	assert.Zero(t, rate0)

	require.NoError(t, s.WarmStock(ctx, ev1, map[string]int{"cat": 30}))
	join(t, s, tokA)
	join(t, s, tokB)
	rate, err := s.AdmissionRatePerSec(ctx, ev1)
	require.NoError(t, err)
	assert.InDelta(t, 2.0/60.0, rate, 1e-9, "2 admit dalam 60 detik terakhir")
}

// ==================================================== QueueStore (Redis mati)

func TestStore_ShouldWrapError_WhenRedisDown(t *testing.T) {
	s, mr := newStore(t, StoreSettings{})
	mr.Close()
	ctx := context.Background()

	calls := map[string]func() error{
		"HasStock":      func() error { _, e := s.HasStock(ctx, ev1); return e },
		"WarmStock":     func() error { return s.WarmStock(ctx, ev1, map[string]int{"c": 1}) },
		"SetStock":      func() error { return s.SetStock(ctx, ev1, "c", 1) },
		"Join":          func() error { _, e := s.Join(ctx, outbound.JoinParams{EventID: ev1, Token: tokA}); return e },
		"State":         func() error { _, e := s.State(ctx, ev1, tokA); return e },
		"Promote":       func() error { _, e := s.Promote(ctx, ev1); return e },
		"Release":       func() error { _, _, e := s.Release(ctx, ev1, tokA); return e },
		"SweepExpired":  func() error { _, e := s.SweepExpired(ctx, ev1); return e },
		"Close":         func() error { _, e := s.Close(ctx, ev1); return e },
		"RegisterEvent": func() error { return s.RegisterEvent(ctx, ev1) },
		"ActiveEvents":  func() error { _, e := s.ActiveEvents(ctx); return e },
		"AdmissionRate": func() error { _, e := s.AdmissionRatePerSec(ctx, ev1); return e },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			assert.Error(t, call(), "error Redis tidak boleh ditelan")
		})
	}
}

func TestConverters(t *testing.T) {
	assert.Equal(t, "abc", asString("abc"))
	assert.Equal(t, "42", asString(int64(42)))
	assert.Equal(t, "3.5", asString(3.5), "tipe lain → fmt.Sprint")

	assert.Equal(t, int64(7), asInt(int64(7)))
	assert.Equal(t, int64(8), asInt("8"))
	assert.Equal(t, int64(0), asInt("bukan-angka"))
	assert.Equal(t, int64(0), asInt(3.5), "tipe tak dikenal → 0")

	assert.Equal(t, []string{"a", "1"}, asStrings([]any{"a", int64(1)}))
	assert.Empty(t, asStrings(nil))
}

// =================================================================== Notifier

func TestNotifier_ShouldPublishTokensAndWildcard(t *testing.T) {
	rdb, _ := newRedis(t)
	ctx := context.Background()
	sub := rdb.Subscribe(ctx, notifyPrefix+ev1)
	t.Cleanup(func() { _ = sub.Close() })
	_, err := sub.Receive(ctx) // konfirmasi subscribe
	require.NoError(t, err)
	n := NewNotifier(rdb)

	require.NoError(t, n.Notify(ctx, ev1, nil), "tanpa token → tidak publish")
	require.NoError(t, n.Notify(ctx, ev1, []string{tokA, tokB}))
	require.NoError(t, n.NotifyAll(ctx, ev1))

	ch := sub.Channel()
	assert.Equal(t, tokA+","+tokB, recv(t, ch))
	assert.Equal(t, "*", recv(t, ch))
}

func recv(t *testing.T, ch <-chan *redis.Message) string {
	t.Helper()
	select {
	case m := <-ch:
		return m.Payload
	case <-time.After(2 * time.Second):
		t.Fatal("tidak ada pesan Pub/Sub")
		return ""
	}
}

func TestNotifier_ShouldReturnError_WhenRedisDown(t *testing.T) {
	rdb, mr := newRedis(t)
	n := NewNotifier(rdb)
	mr.Close()

	assert.Error(t, n.Notify(context.Background(), ev1, []string{tokA}))
	assert.Error(t, n.NotifyAll(context.Background(), ev1))
}

// ========================================================================= Hub

func signalled(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestHub_Dispatch_ShouldWakeOnlyTargetedTokens(t *testing.T) {
	h := NewHub(nil)
	chA, _ := h.Subscribe(ev1, tokA)
	chB, _ := h.Subscribe(ev1, tokB)
	chOther, _ := h.Subscribe("event-lain", tokA)

	h.dispatch(ev1, tokA)

	assert.True(t, signalled(chA))
	assert.False(t, signalled(chB))
	assert.False(t, signalled(chOther), "event lain tidak ikut terbangun")
}

func TestHub_Dispatch_ShouldSupportMultipleTokensAndWildcard(t *testing.T) {
	h := NewHub(nil)
	chA, _ := h.Subscribe(ev1, tokA)
	chB, _ := h.Subscribe(ev1, tokB)
	chC, _ := h.Subscribe(ev1, tokC)

	h.dispatch(ev1, tokA+","+tokB)
	assert.True(t, signalled(chA))
	assert.True(t, signalled(chB))
	assert.False(t, signalled(chC))

	h.dispatch(ev1, "*")
	assert.True(t, signalled(chA) && signalled(chB) && signalled(chC), "'*' membangunkan semua koneksi event")
}

func TestHub_Dispatch_ShouldCoalesceBurstsIntoOneWake(t *testing.T) {
	h := NewHub(nil)
	ch, _ := h.Subscribe(ev1, tokA)

	h.dispatch(ev1, tokA)
	h.dispatch(ev1, tokA)
	h.dispatch(ev1, tokA)

	assert.True(t, signalled(ch))
	assert.False(t, signalled(ch), "sinyal beruntun dilebur jadi satu")
}

func TestHub_ShouldIgnoreUnknownEventOrToken(t *testing.T) {
	h := NewHub(nil)

	assert.NotPanics(t, func() {
		h.dispatch("tak-ada", "*")
		h.dispatch("tak-ada", tokA)
	})
	h.Subscribe(ev1, tokA)
	assert.NotPanics(t, func() { h.dispatch(ev1, tokB) })
}

func TestHub_Unsubscribe_ShouldCleanUpAndStopWaking(t *testing.T) {
	h := NewHub(nil)
	ch1, unsub1 := h.Subscribe(ev1, tokA)
	ch2, unsub2 := h.Subscribe(ev1, tokA) // dua koneksi untuk token yang sama (mis. dua tab)

	unsub1()
	h.dispatch(ev1, tokA)
	assert.False(t, signalled(ch1), "yang sudah unsubscribe tidak dibangunkan")
	assert.True(t, signalled(ch2))
	assert.Len(t, h.subs, 1)

	unsub2()
	assert.Empty(t, h.subs, "map harus bersih agar tidak bocor memori")
	assert.NotPanics(t, func() { h.dispatch(ev1, tokA) })
}

func TestHub_Run_ShouldRelayPubSubToSubscribers(t *testing.T) {
	rdb, mr := newRedis(t)
	h := NewHub(rdb)
	ch, unsub := h.Subscribe(ev1, tokA)
	defer unsub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)
	require.Eventually(t, func() bool { return mr.PubSubNumPat() > 0 }, 2*time.Second, 10*time.Millisecond,
		"hub harus berlangganan pola queue:notify:*")

	require.NoError(t, NewNotifier(rdb).Notify(ctx, ev1, []string{tokA}))

	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("koneksi SSE tidak terbangun oleh sinyal Pub/Sub")
	}
}
