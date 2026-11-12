package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/refreshTechRusuh/queue-service/internal/config"
)

const evt = "0190a8f0-1111-7000-8000-000000000001"

type rig struct {
	h  http.Handler
	mr *miniredis.Miniredis
}

// newRig merakit service dengan miniredis dan pool Postgres malas (tidak pernah dipakai
// karena stok sudah di-warm di Redis). Kafka dimatikan.
func newRig(t *testing.T, mutate func(*config.Config)) *rig {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?connect_timeout=1")
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	t.Setenv("KAFKA_ENABLED", "false")
	cfg, err := config.Load()
	require.NoError(t, err)
	cfg.Queue.SweepInterval = time.Hour // sweeper tidak ikut campur di test route
	if mutate != nil {
		mutate(cfg)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h, closeFn := New(ctx, cfg, rdb, pool)
	t.Cleanup(closeFn)

	mr.HSet("stock:{"+evt+"}", "cat-1", "30") // warm-up: tak perlu DB
	return &rig{h: h, mr: mr}
}

func (r *rig) do(method, path string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	r.h.ServeHTTP(rr, httptest.NewRequest(method, path, nil))
	return rr
}

func TestNew_ShouldServeHealthCheck_WithTransactionID(t *testing.T) {
	r := newRig(t, nil)

	rr := r.do(http.MethodGet, "/sys/ping")

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "queue-service ok")
	assert.NotEmpty(t, rr.Header().Get("X-Transaction-Id"))
}

func TestNew_HealthCheck_ShouldReport503_WhenRedisDown(t *testing.T) {
	r := newRig(t, nil)
	r.mr.Close()

	rr := r.do(http.MethodGet, "/sys/ping")

	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
	assert.Contains(t, rr.Body.String(), "redis unavailable")
}

func TestNew_ShouldWireJoinAndStatusRoutes(t *testing.T) {
	r := newRig(t, nil)

	join := r.do(http.MethodPost, "/api/queue/v1/event/"+evt+"/join")

	require.Equal(t, http.StatusOK, join.Code, join.Body.String())
	assert.Contains(t, join.Body.String(), `"status":"ADMITTED"`)
	assert.Contains(t, join.Body.String(), `"accessToken"`)
}

func TestNew_ShouldTraceJoin_ButNotSSEStreamOrHealthCheck(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
		_ = tp.Shutdown(context.Background())
	})
	r := newRig(t, nil)

	join := r.do(http.MethodPost, "/api/queue/v1/event/"+evt+"/join")
	require.Equal(t, http.StatusOK, join.Code)
	var env struct {
		Data struct {
			QueueToken string `json:"queueToken"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(join.Body.Bytes(), &env))
	r.do(http.MethodGet, "/sys/ping")
	stream := r.do(http.MethodGet, "/api/queue/v1/event/"+evt+"/"+env.Data.QueueToken+"/stream")

	assert.Equal(t, http.StatusOK, stream.Code)
	assert.Contains(t, stream.Body.String(), "event: admitted", "SSE tetap berfungsi lewat rantai middleware")
	assert.Len(t, rec.Ended(), 1, "hanya join yang di-trace; health check & SSE dikecualikan")
}

func TestNew_ShouldRateLimitJoin_PerIP(t *testing.T) {
	r := newRig(t, func(c *config.Config) { c.Queue.JoinRatePerMin = 2 })

	var last int
	for i := 0; i < 3; i++ {
		last = r.do(http.MethodPost, "/api/queue/v1/event/"+evt+"/join").Code
	}

	assert.Equal(t, http.StatusTooManyRequests, last)
}

func TestNew_ShouldReturn404_ForUnknownRoute_AndRejectWrongMethod(t *testing.T) {
	r := newRig(t, nil)

	assert.Equal(t, http.StatusNotFound, r.do(http.MethodGet, "/tidak/ada").Code)
	assert.Equal(t, http.StatusMethodNotAllowed, r.do(http.MethodDelete, "/api/queue/v1/event/"+evt+"/join").Code)
	// GET .../join cocok dengan rute status (token bernama "join") → ditolak validasi token.
	assert.Equal(t, http.StatusBadRequest, r.do(http.MethodGet, "/api/queue/v1/event/"+evt+"/join").Code)
}

func TestNew_ShouldReturnNoopClose_WhenKafkaDisabled(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db")
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	t.Setenv("KAFKA_ENABLED", "false")
	cfg, err := config.Load()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, closeFn := New(ctx, cfg, rdb, pool)

	assert.NotPanics(t, closeFn)
}

// ------------------------------------------------------------------ sweeper

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestRunSweeper_ShouldCallSweepPeriodically_UntilContextCancelled(t *testing.T) {
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		runSweeper(ctx, func(context.Context) error { calls.Add(1); return nil }, 10*time.Millisecond, quiet)
		close(done)
	}()
	require.Eventually(t, func() bool { return calls.Load() >= 3 }, 2*time.Second, 5*time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sweeper harus berhenti saat context dibatalkan")
	}
}

func TestRunSweeper_ShouldKeepRunning_WhenSweepFails(t *testing.T) {
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go runSweeper(ctx, func(context.Context) error { calls.Add(1); return errors.New("redis putus") }, 10*time.Millisecond, quiet)

	require.Eventually(t, func() bool { return calls.Load() >= 3 }, 2*time.Second, 5*time.Millisecond,
		"satu sweep gagal tidak boleh mematikan sweeper (kalau mati, antrian macet)")
}

// ---------------------------------------------------------------- keamanan

func TestNew_ShouldSetSecurityHeaders_OnEveryKindOfResponse(t *testing.T) {
	r := newRig(t, nil)

	for name, rr := range map[string]*httptest.ResponseRecorder{
		"200 health":        r.do(http.MethodGet, "/sys/ping"),
		"404 route tak ada": r.do(http.MethodGet, "/tidak/ada"),
		"400 id tak valid":  r.do(http.MethodPost, "/api/queue/v1/event/bukan-uuid/join"),
		"preflight OPTIONS": r.do(http.MethodOptions, "/api/queue/v1/event/"+evt+"/join"),
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, "nosniff", rr.Header().Get("X-Content-Type-Options"))
			assert.Equal(t, "default-src 'none'", rr.Header().Get("Content-Security-Policy"))
			assert.Equal(t, "no-store", rr.Header().Get("Cache-Control"))
			assert.NotEmpty(t, rr.Header().Get("Strict-Transport-Security"))
			assert.Empty(t, rr.Header().Get("X-Frame-Options"))
		})
	}
}

func TestNew_ShouldReturn413_WithCORS_WhenBodyOverOneMB(t *testing.T) {
	r := newRig(t, nil)
	req := httptest.NewRequest(http.MethodPost, "/api/queue/v1/event/"+evt+"/join", strings.NewReader(strings.Repeat("a", (1<<20)+1)))
	req.Header.Set("Origin", "https://fe.example")
	rr := httptest.NewRecorder()

	r.h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusRequestEntityTooLarge, rr.Code)
	assert.Equal(t, "https://fe.example", rr.Header().Get("Access-Control-Allow-Origin"))
}

func TestNew_SSEStream_ShouldKeepNoCache_NotOverwrittenByNoStoreDefault(t *testing.T) {
	r := newRig(t, nil)
	join := r.do(http.MethodPost, "/api/queue/v1/event/"+evt+"/join")
	require.Equal(t, http.StatusOK, join.Code)
	var env struct {
		Data struct {
			QueueToken string `json:"queueToken"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(join.Body.Bytes(), &env))

	stream := r.do(http.MethodGet, "/api/queue/v1/event/"+evt+"/"+env.Data.QueueToken+"/stream")

	assert.Equal(t, "no-cache", stream.Header().Get("Cache-Control"), "SSE butuh no-cache dari handlernya sendiri")
	assert.Equal(t, "text/event-stream", stream.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", stream.Header().Get("X-Content-Type-Options"))
}
