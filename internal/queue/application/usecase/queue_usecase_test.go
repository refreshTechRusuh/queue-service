// Integration test usecase antrian terhadap Redis SUNGGUHAN (DB index 15).
// Atomisitas Lua hanya bisa dibuktikan di Redis asli — mock tak akan menangkap race.
// Test di-skip otomatis bila Redis tidak tersedia (lihat newEnv).
// DB 15 khusus paket ini (paket http memakai DB 14) karena paket test berjalan paralel.
package usecase_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/refreshTechRusuh/queue-service/internal/queue/application/usecase"
	"github.com/refreshTechRusuh/queue-service/internal/queue/domain/dto"
	"github.com/refreshTechRusuh/queue-service/internal/queue/domain/entity"
	"github.com/refreshTechRusuh/queue-service/internal/queue/infrastructure/cache"
	"github.com/refreshTechRusuh/queue-service/internal/queue/infrastructure/token"
	"github.com/refreshTechRusuh/queue-service/pkg/apperror"
)

const testSecret = "test-secret"

// ---- fakes ----

type fakeStock struct {
	mu    sync.Mutex
	data  map[string]map[string]int
	calls int           // jumlah LoadStock dipanggil (uji singleflight)
	delay time.Duration // simulasi query DB lambat
}

func (f *fakeStock) set(eventID string, stock map[string]int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[eventID] = stock
}

func (f *fakeStock) LoadStock(_ context.Context, eventID string) (map[string]int, error) {
	f.mu.Lock()
	f.calls++
	d := f.delay
	f.mu.Unlock()
	time.Sleep(d)
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]int{}
	for k, v := range f.data[eventID] {
		out[k] = v
	}
	return out, nil
}

type fakeNotifier struct {
	mu       sync.Mutex
	notified map[string][]string
	all      map[string]int
}

func (f *fakeNotifier) Notify(_ context.Context, eventID string, tokens []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.notified[eventID] = append(f.notified[eventID], tokens...)
	return nil
}

func (f *fakeNotifier) NotifyAll(_ context.Context, eventID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.all[eventID]++
	return nil
}

// ---- env ----

type env struct {
	uc       *usecase.QueueUsecase
	rdb      *redis.Client
	stock    *fakeStock
	notifier *fakeNotifier
}

type opts struct {
	globalCap  int
	queueCap   int
	bookingTTL time.Duration
}

func newEnv(t *testing.T, o opts) *env {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis tidak tersedia di %s: %v", addr, err)
	}
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flush redis test db: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	if o.bookingTTL == 0 {
		o.bookingTTL = 5 * time.Minute
	}
	store := cache.NewQueueStore(rdb, cache.StoreSettings{
		GlobalBookingCap: o.globalCap,
		QueueCap:         o.queueCap,
		BookingTTL:       o.bookingTTL,
		StateTTL:         time.Hour,
		ClosedTTL:        time.Hour,
	})
	st := &fakeStock{data: map[string]map[string]int{}}
	nt := &fakeNotifier{notified: map[string][]string{}, all: map[string]int{}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	uc := usecase.NewQueueUsecase(store, st, nt, token.NewSigner(testSecret),
		usecase.Settings{BookingTTL: o.bookingTTL}, log)
	return &env{uc: uc, rdb: rdb, stock: st, notifier: nt}
}

// newEvent membuat event baru dengan stok (satu kategori).
func (e *env) newEvent(stock int) string {
	id := uuid.NewString()
	e.stock.set(id, map[string]int{"cat-1": stock})
	return id
}

func (e *env) occupancy(t *testing.T, eventID string) int {
	t.Helper()
	n, err := e.rdb.Get(context.Background(), "admission:{"+eventID+"}").Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatalf("baca occupancy: %v", err)
	}
	return n
}

func (e *env) join(t *testing.T, eventID string) dto.QueueStatusResponse {
	t.Helper()
	res, err := e.uc.Join(context.Background(), eventID)
	if err != nil {
		t.Fatalf("Join(%s) error tak terduga: %v", eventID, err)
	}
	return res
}

func (e *env) status(t *testing.T, eventID, tok string) dto.QueueStatusResponse {
	t.Helper()
	res, err := e.uc.Status(context.Background(), eventID, tok)
	if err != nil {
		t.Fatalf("Status(%s) error tak terduga: %v", tok, err)
	}
	return res
}

func errCode(err error) apperror.ErrorCode {
	var ae *apperror.AppError
	if errors.As(err, &ae) {
		return ae.Code
	}
	return ""
}

// ============================================================
// Anti-PHP (bukti sidang, Req 23)
// ============================================================

// Stok 1 + 2 orang join BERSAMAAN → tepat 1 ADMITTED, 1 QUEUED. Diulang 50x
// dengan event berbeda supaya race benar-benar teruji.
func TestJoin_ShouldAdmitExactlyOne_WhenStock1AndTwoJoinConcurrently(t *testing.T) {
	e := newEnv(t, opts{})
	for i := 0; i < 50; i++ {
		eventID := e.newEvent(1)
		start := make(chan struct{})
		results := make(chan dto.QueueStatusResponse, 2)
		var wg sync.WaitGroup
		for j := 0; j < 2; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				res, err := e.uc.Join(context.Background(), eventID)
				if err != nil {
					t.Errorf("join error: %v", err)
					return
				}
				results <- res
			}()
		}
		close(start)
		wg.Wait()
		close(results)

		admitted, queued := 0, 0
		for r := range results {
			switch r.Status {
			case "ADMITTED":
				admitted++
			case "QUEUED":
				queued++
			}
		}
		if admitted != 1 || queued != 1 {
			t.Fatalf("iterasi %d: admitted=%d queued=%d; mau 1 dan 1", i, admitted, queued)
		}
	}
}

// 200 orang join bersamaan saat cap=10 → occupancy TIDAK PERNAH > 10.
func TestJoin_ShouldNeverExceedCap_WhenManyJoinConcurrently(t *testing.T) {
	e := newEnv(t, opts{})
	eventID := e.newEvent(30) // cap = floor(30/3) = 10

	const n = 200
	start := make(chan struct{})
	results := make(chan dto.QueueStatusResponse, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := e.uc.Join(context.Background(), eventID)
			if err != nil {
				t.Errorf("join error: %v", err)
				return
			}
			results <- res
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	admitted := 0
	positions := map[int64]bool{}
	for r := range results {
		if r.Status == "ADMITTED" {
			admitted++
		} else {
			if positions[r.Position] {
				t.Errorf("posisi %d dibagikan ke dua orang", r.Position)
			}
			positions[r.Position] = true
		}
	}
	if admitted != 10 {
		t.Errorf("admitted = %d; mau 10", admitted)
	}
	if occ := e.occupancy(t, eventID); occ != 10 {
		t.Errorf("occupancy = %d; mau 10", occ)
	}
	if len(positions) != n-10 {
		t.Errorf("jumlah yang antre = %d; mau %d", len(positions), n-10)
	}
}

// Rumus cap di Lua HARUS identik dengan entity.CapByStock.
func TestCap_LuaShouldMatchDomainFormula(t *testing.T) {
	stocks := []int{0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 31, 100}
	for _, globalCap := range []int{0, 5} {
		for _, s := range stocks {
			t.Run(fmt.Sprintf("stok=%d_plafon=%d", s, globalCap), func(t *testing.T) {
				e := newEnv(t, opts{globalCap: globalCap})
				eventID := e.newEvent(s)
				want := entity.CapByStock(s, globalCap)
				admitted := 0
				for i := 0; i < want+5; i++ {
					if e.join(t, eventID).Status == "ADMITTED" {
						admitted++
					}
				}
				if admitted != want {
					t.Errorf("stok %d plafon %d: Lua meng-admit %d; rumus Go %d", s, globalCap, admitted, want)
				}
			})
		}
	}
}

// ============================================================
// FCFS & posisi
// ============================================================

func TestJoin_ShouldAssignPositionsInArrivalOrder_AndKeepThemOnRefresh(t *testing.T) {
	e := newEnv(t, opts{})
	eventID := e.newEvent(0) // cap 0 → semua antre

	var tokens []string
	for i := 1; i <= 5; i++ {
		r := e.join(t, eventID)
		if r.Status != "QUEUED" || r.Position != int64(i) {
			t.Fatalf("join ke-%d: status=%s posisi=%d; mau QUEUED posisi %d", i, r.Status, r.Position, i)
		}
		tokens = append(tokens, r.QueueToken)
	}
	// Refresh (Req 3.4): token yang sama → posisi yang sama.
	for i, tok := range tokens {
		if got := e.status(t, eventID, tok).Position; got != int64(i+1) {
			t.Errorf("refresh token ke-%d: posisi %d; mau %d", i+1, got, i+1)
		}
	}
}

// ============================================================
// Promosi gelombang
// ============================================================

// Lepas 3 kursi → tepat 3 orang TERDEPAN dipromosikan sekaligus, sisanya maju.
func TestRelease_ShouldPromoteExactlyFrontM_WhenMSeatsReleased(t *testing.T) {
	e := newEnv(t, opts{})
	eventID := e.newEvent(30) // cap 10

	var tokens []string
	for i := 0; i < 15; i++ {
		tokens = append(tokens, e.join(t, eventID).QueueToken)
	}
	for i := 0; i < 3; i++ {
		if err := e.uc.OnSlotReleased(context.Background(), eventID, tokens[i]); err != nil {
			t.Fatalf("release: %v", err)
		}
	}

	for i := 10; i < 13; i++ {
		if s := e.status(t, eventID, tokens[i]).Status; s != "ADMITTED" {
			t.Errorf("token antrean ke-%d: %s; mau ADMITTED", i-9, s)
		}
	}
	for i, want := range map[int]int64{13: 1, 14: 2} {
		if got := e.status(t, eventID, tokens[i]); got.Status != "QUEUED" || got.Position != want {
			t.Errorf("token ke-%d: %s posisi %d; mau QUEUED posisi %d", i, got.Status, got.Position, want)
		}
	}
	if occ := e.occupancy(t, eventID); occ != 10 {
		t.Errorf("occupancy = %d; mau tetap 10", occ)
	}
	if got := len(e.notifier.notified[eventID]); got != 3 {
		t.Errorf("token yang dinotifikasi = %d; mau 3", got)
	}
}

func TestRelease_ShouldBeIdempotent_WhenSameTokenReleasedTwice(t *testing.T) {
	e := newEnv(t, opts{})
	eventID := e.newEvent(6) // cap 2
	a := e.join(t, eventID).QueueToken
	e.join(t, eventID)

	for i := 0; i < 2; i++ {
		if err := e.uc.OnSlotReleased(context.Background(), eventID, a); err != nil {
			t.Fatalf("release: %v", err)
		}
	}
	if occ := e.occupancy(t, eventID); occ != 1 {
		t.Errorf("occupancy = %d; mau 1 (DECR hanya sekali)", occ)
	}
}

// Stok naik (mis. hold dilepas) → cap naik → gelombang promosi.
func TestStockChanged_ShouldPromoteWave_WhenStockIncreases(t *testing.T) {
	e := newEnv(t, opts{})
	eventID := e.newEvent(3) // cap 1
	var tokens []string
	for i := 0; i < 4; i++ {
		tokens = append(tokens, e.join(t, eventID).QueueToken)
	}
	if err := e.uc.OnStockChanged(context.Background(), eventID, "cat-1", 12); err != nil { // cap 4
		t.Fatalf("stock changed: %v", err)
	}
	for i, tok := range tokens {
		if s := e.status(t, eventID, tok).Status; s != "ADMITTED" {
			t.Errorf("token ke-%d: %s; mau ADMITTED", i, s)
		}
	}
}

// Stok turun tidak mengusir orang yang sudah di dalam, dan tidak menambah admit baru.
func TestStockChanged_ShouldNotAdmitMore_WhenStockDecreases(t *testing.T) {
	e := newEnv(t, opts{})
	eventID := e.newEvent(30) // cap 10
	for i := 0; i < 10; i++ {
		e.join(t, eventID)
	}
	if err := e.uc.OnStockChanged(context.Background(), eventID, "cat-1", 3); err != nil { // cap 1
		t.Fatalf("stock changed: %v", err)
	}
	if r := e.join(t, eventID); r.Status != "QUEUED" {
		t.Errorf("pendatang baru: %s; mau QUEUED", r.Status)
	}
	if occ := e.occupancy(t, eventID); occ != 10 {
		t.Errorf("occupancy = %d; mau 10", occ)
	}
}

// ============================================================
// Sweeper: kursi orang yang tak pernah submit tidak boleh bocor
// ============================================================

func TestSweep_ShouldReleaseExpiredSeat_AndPromoteNext(t *testing.T) {
	e := newEnv(t, opts{bookingTTL: 300 * time.Millisecond})
	eventID := e.newEvent(3) // cap 1
	a := e.join(t, eventID)
	b := e.join(t, eventID)
	if a.Status != "ADMITTED" || b.Status != "QUEUED" {
		t.Fatalf("awal: a=%s b=%s; mau ADMITTED, QUEUED", a.Status, b.Status)
	}

	time.Sleep(450 * time.Millisecond)
	if err := e.uc.Sweep(context.Background()); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	if s := e.status(t, eventID, a.QueueToken).Status; s != "EXPIRED" {
		t.Errorf("a: %s; mau EXPIRED", s)
	}
	if s := e.status(t, eventID, b.QueueToken).Status; s != "ADMITTED" {
		t.Errorf("b: %s; mau ADMITTED", s)
	}
	if occ := e.occupancy(t, eventID); occ != 1 {
		t.Errorf("occupancy = %d; mau 1", occ)
	}
}

// ============================================================
// Queue cap (503) vs sold out (410)
// ============================================================

func TestJoin_ShouldReturnQueueFull_WhenQueueCapReached(t *testing.T) {
	e := newEnv(t, opts{queueCap: 2})
	eventID := e.newEvent(0)
	e.join(t, eventID)
	e.join(t, eventID)
	_, err := e.uc.Join(context.Background(), eventID)
	if errCode(err) != apperror.ErrQueueFull {
		t.Fatalf("join ke-3: err=%v; mau QUEUE_FULL", err)
	}
}

func TestSoldOut_ShouldCloseQueue_AndRejectNewJoin(t *testing.T) {
	e := newEnv(t, opts{})
	eventID := e.newEvent(0)
	tok := e.join(t, eventID).QueueToken

	for i := 0; i < 2; i++ { // idempoten
		if err := e.uc.OnSoldOut(context.Background(), eventID); err != nil {
			t.Fatalf("sold out: %v", err)
		}
	}
	if s := e.status(t, eventID, tok).Status; s != "SOLD_OUT" {
		t.Errorf("token di antrian: %s; mau SOLD_OUT", s)
	}
	if _, err := e.uc.Join(context.Background(), eventID); errCode(err) != apperror.ErrSoldOut {
		t.Errorf("join setelah sold out: err=%v; mau SOLD_OUT", err)
	}
	if e.notifier.all[eventID] == 0 {
		t.Error("NotifyAll tidak dipanggil saat sold out")
	}
}

// ============================================================
// Validasi & not found
// ============================================================

func TestValidation_ShouldRejectBadInput(t *testing.T) {
	e := newEnv(t, opts{})
	eventID := e.newEvent(3)
	ctx := context.Background()

	cases := []struct {
		name string
		call func() error
		want apperror.ErrorCode
	}{
		{"eventId bukan UUID", func() error { _, err := e.uc.Join(ctx, "abc"); return err }, apperror.ErrValidation},
		{"eventId format kurung kurawal", func() error {
			_, err := e.uc.Join(ctx, "{"+eventID+"}")
			return err
		}, apperror.ErrValidation},
		{"event tidak ada", func() error { _, err := e.uc.Join(ctx, uuid.NewString()); return err }, apperror.ErrNotFound},
		{"queueToken format salah", func() error { _, err := e.uc.Status(ctx, eventID, "ngaco"); return err }, apperror.ErrValidation},
		{"queueToken tidak dikenal", func() error {
			_, err := e.uc.Status(ctx, eventID, "q_"+uuid.NewString())
			return err
		}, apperror.ErrNotFound},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := errCode(c.call()); got != c.want {
				t.Errorf("kode error = %q; mau %q", got, c.want)
			}
		})
	}
}

// ============================================================
// Kontrak Access Token dengan booking-service
// ============================================================

// Claim ini meniru AccessTokenClaims di booking-service/pkg/middleware/auth.go.
type bookingAccessTokenClaims struct {
	EventID             string `json:"eventId"`
	QueueToken          string `json:"queueToken"`
	NIK                 string `json:"nik,omitempty"` // opsional di booking; queue tidak mengisinya
	BookingTTLExpiresAt int64  `json:"bookingTtlExpiresAt"`
	jwt.RegisteredClaims
}

func TestAccessToken_ShouldBeVerifiableByBookingService_AndStableOnRefresh(t *testing.T) {
	e := newEnv(t, opts{})
	eventID := e.newEvent(3)
	res, err := e.uc.Join(context.Background(), eventID)
	if err != nil || res.Status != "ADMITTED" {
		t.Fatalf("join: status=%s err=%v; mau ADMITTED", res.Status, err)
	}

	claims := &bookingAccessTokenClaims{}
	_, err = jwt.ParseWithClaims(res.AccessToken, claims, func(tk *jwt.Token) (any, error) {
		if _, ok := tk.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrSignatureInvalid
		}
		return []byte(testSecret), nil
	})
	if err != nil {
		t.Fatalf("booking-service gagal memvalidasi token: %v", err)
	}
	if claims.EventID != eventID || claims.QueueToken != res.QueueToken || claims.NIK != "" {
		t.Errorf("claim salah: %+v", claims)
	}
	if claims.BookingTTLExpiresAt != claims.ExpiresAt.Unix() {
		t.Errorf("bookingTtlExpiresAt %d ≠ exp %d", claims.BookingTTLExpiresAt, claims.ExpiresAt.Unix())
	}
	if ttl := time.Until(claims.ExpiresAt.Time); ttl < 4*time.Minute || ttl > 5*time.Minute+time.Second {
		t.Errorf("sisa TTL %v; mau sekitar 5 menit", ttl)
	}

	// Refresh mengembalikan token yang SAMA (dibangun ulang deterministik dari state).
	if again := e.status(t, eventID, res.QueueToken).AccessToken; again != res.AccessToken {
		t.Error("access token berubah saat refresh; mau identik")
	}
}

// ============================================================
// Warm-up stok saat gerbang dibuka (singleflight)
// ============================================================

// 300 orang join bersamaan saat cache stok masih kosong → DB dibaca SEKALI saja.
func TestJoin_ShouldLoadStockOnce_WhenManyJoinOnColdCache(t *testing.T) {
	e := newEnv(t, opts{})
	e.stock.delay = 100 * time.Millisecond
	eventID := e.newEvent(30)

	const n = 300
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := e.uc.Join(context.Background(), eventID); err != nil {
				t.Errorf("join error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if e.stock.calls != 1 {
		t.Errorf("LoadStock dipanggil %d kali; mau 1", e.stock.calls)
	}
	if occ := e.occupancy(t, eventID); occ != 10 {
		t.Errorf("occupancy = %d; mau 10", occ)
	}
}
