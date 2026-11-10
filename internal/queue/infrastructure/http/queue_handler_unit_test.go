package http_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/refreshTechRusuh/queue-service/internal/queue/domain/dto"
	queuehttp "github.com/refreshTechRusuh/queue-service/internal/queue/infrastructure/http"
	"github.com/refreshTechRusuh/queue-service/pkg/apperror"
)

// Unit test adapter HTTP dengan usecase & waker tiruan (tanpa Redis).
// (queue_handler_test.go = end-to-end terhadap Redis/miniredis.)

const (
	evt = "0190a8f0-1111-7000-8000-000000000001"
	tok = "q_0190a8f0-2222-7000-8000-000000000002"
)

// stubUC mengembalikan jawaban berurutan untuk Status; elemen terakhir diulang.
type stubUC struct {
	mu       sync.Mutex
	statuses []func() (dto.QueueStatusResponse, error)
	calls    int
	join     func() (dto.QueueStatusResponse, error)
}

func (s *stubUC) Join(context.Context, string) (dto.QueueStatusResponse, error) { return s.join() }
func (s *stubUC) Status(context.Context, string, string) (dto.QueueStatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.calls
	if i >= len(s.statuses) {
		i = len(s.statuses) - 1
	}
	s.calls++
	return s.statuses[i]()
}
func (s *stubUC) OnStockChanged(context.Context, string, string, int) error { return nil }
func (s *stubUC) OnSlotReleased(context.Context, string, string) error      { return nil }
func (s *stubUC) OnSoldOut(context.Context, string) error                   { return nil }
func (s *stubUC) Sweep(context.Context) error                               { return nil }

func resp(status string) func() (dto.QueueStatusResponse, error) {
	return func() (dto.QueueStatusResponse, error) {
		return dto.QueueStatusResponse{Status: status, QueueToken: tok}, nil
	}
}
func queued(pos int64) func() (dto.QueueStatusResponse, error) {
	return func() (dto.QueueStatusResponse, error) {
		return dto.QueueStatusResponse{Status: "QUEUED", QueueToken: tok, Position: pos}, nil
	}
}
func fail(err error) func() (dto.QueueStatusResponse, error) {
	return func() (dto.QueueStatusResponse, error) { return dto.QueueStatusResponse{}, err }
}

// stubWaker: wake dikendalikan test; unsubscribe dicatat.
type stubWaker struct {
	ch           chan struct{}
	unsubscribed chan struct{}
}

func newWaker() *stubWaker {
	return &stubWaker{ch: make(chan struct{}, 1), unsubscribed: make(chan struct{}, 1)}
}
func (w *stubWaker) Subscribe(string, string) (<-chan struct{}, func()) {
	return w.ch, func() { w.unsubscribed <- struct{}{} }
}

func newMux(uc *stubUC, w *stubWaker, tick time.Duration) *http.ServeMux {
	h := queuehttp.NewHandler(uc, w, tick)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/queue/v1/event/{eventId}/join", h.Join)
	mux.HandleFunc("GET /api/queue/v1/event/{eventId}/{queueToken}", h.Status)
	mux.HandleFunc("GET /api/queue/v1/event/{eventId}/{queueToken}/stream", h.Stream)
	return mux
}

func get(mux http.Handler, path string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
	return rr
}

const streamPath = "/api/queue/v1/event/" + evt + "/" + tok + "/stream"

// ------------------------------------------------------- Join & Status JSON

func TestJoinHandler_ShouldUseStatusSpecificMessage(t *testing.T) {
	cases := map[string]string{
		"ADMITTED": "akses diberikan",
		"QUEUED":   "anda dalam antrian",
		"SOLD_OUT": "tiket sudah habis",
		"EXPIRED":  "sesi antrian berakhir",
	}
	for status, wantMsg := range cases {
		t.Run(status, func(t *testing.T) {
			uc := &stubUC{join: func() (dto.QueueStatusResponse, error) {
				return dto.QueueStatusResponse{Status: status, QueueToken: tok}, nil
			}}
			rr := httptest.NewRecorder()

			newMux(uc, newWaker(), time.Hour).ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/queue/v1/event/"+evt+"/join", nil))

			assert.Equal(t, http.StatusOK, rr.Code)
			assert.Contains(t, rr.Body.String(), wantMsg)
		})
	}
}

func TestJoinHandler_ShouldMapUsecaseErrors(t *testing.T) {
	cases := map[int]error{
		http.StatusGone:               apperror.SoldOut(),
		http.StatusServiceUnavailable: apperror.QueueFull(),
		http.StatusBadRequest:         apperror.Validation("x"),
	}
	for want, err := range cases {
		uc := &stubUC{join: func() (dto.QueueStatusResponse, error) { return dto.QueueStatusResponse{}, err }}
		rr := httptest.NewRecorder()

		newMux(uc, newWaker(), time.Hour).ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/queue/v1/event/"+evt+"/join", nil))

		assert.Equal(t, want, rr.Code)
	}
}

func TestStatusHandler(t *testing.T) {
	t.Run("sukses", func(t *testing.T) {
		rr := get(newMux(&stubUC{statuses: []func() (dto.QueueStatusResponse, error){queued(3)}}, newWaker(), time.Hour),
			"/api/queue/v1/event/"+evt+"/"+tok)
		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), `"position":3`)
	})
	t.Run("token tak dikenal → 404", func(t *testing.T) {
		rr := get(newMux(&stubUC{statuses: []func() (dto.QueueStatusResponse, error){fail(apperror.NotFound("queueToken"))}}, newWaker(), time.Hour),
			"/api/queue/v1/event/"+evt+"/"+tok)
		assert.Equal(t, http.StatusNotFound, rr.Code)
	})
}

// ------------------------------------------------------------------- Stream

func TestStream_ShouldReturnJSONError_BeforeOpeningStream(t *testing.T) {
	uc := &stubUC{statuses: []func() (dto.QueueStatusResponse, error){fail(apperror.NotFound("queueToken"))}}

	rr := get(newMux(uc, newWaker(), time.Hour), streamPath)

	assert.Equal(t, http.StatusNotFound, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"), "error sebelum SSE dibuka berupa envelope biasa")
}

func TestStream_ShouldCloseImmediately_OnTerminalStates(t *testing.T) {
	cases := []struct {
		name     string
		first    func() (dto.QueueStatusResponse, error)
		wantText []string
	}{
		{"admitted", func() (dto.QueueStatusResponse, error) {
			return dto.QueueStatusResponse{Status: "ADMITTED", AccessToken: "jwt-abc", BookingExpiresAt: "2026-11-10T10:00:00Z"}, nil
		}, []string{"event: admitted", `"accessToken":"jwt-abc"`, `"bookingExpiresAt":"2026-11-10T10:00:00Z"`}},
		{"sold_out", resp("SOLD_OUT"), []string{"event: sold_out", "Tiket sudah habis"}},
		{"expired", resp("EXPIRED"), []string{"event: expired", "Sesi antrian sudah berakhir"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWaker()
			rr := get(newMux(&stubUC{statuses: []func() (dto.QueueStatusResponse, error){tc.first}}, w, time.Hour), streamPath)

			assert.Equal(t, http.StatusOK, rr.Code)
			assert.Equal(t, "text/event-stream", rr.Header().Get("Content-Type"))
			assert.Equal(t, "no", rr.Header().Get("X-Accel-Buffering"))
			for _, s := range tc.wantText {
				assert.Contains(t, rr.Body.String(), s)
			}
			select {
			case <-w.unsubscribed:
			case <-time.After(time.Second):
				t.Fatal("harus unsubscribe dari hub saat stream selesai (cegah kebocoran)")
			}
		})
	}
}

func TestStream_ShouldPushPositionOnlyOnChange_PingOtherwise_ThenAdmit(t *testing.T) {
	uc := &stubUC{statuses: []func() (dto.QueueStatusResponse, error){
		queued(5), // pra-cek → event position 5
		queued(5), // dibangunkan, posisi sama → ": ping"
		queued(3), // posisi berubah → event position 3
		func() (dto.QueueStatusResponse, error) {
			return dto.QueueStatusResponse{Status: "ADMITTED", AccessToken: "jwt"}, nil
		},
	}}
	w := newWaker()
	go func() {
		for i := 0; i < 3; i++ {
			w.ch <- struct{}{}
		}
	}()

	rr := get(newMux(uc, w, time.Hour), streamPath)

	body := rr.Body.String()
	assert.Equal(t, 1, strings.Count(body, `"position":5`), "posisi yang sama tidak dikirim ulang")
	assert.Equal(t, 1, strings.Count(body, ": ping"))
	assert.Equal(t, 1, strings.Count(body, `"position":3`))
	assert.Contains(t, body, "event: admitted")
	assert.Less(t, strings.Index(body, `"position":5`), strings.Index(body, `"position":3`))
	assert.Less(t, strings.Index(body, `"position":3`), strings.Index(body, "event: admitted"))
}

func TestStream_ShouldRecheckOnTick_WithoutWake(t *testing.T) {
	uc := &stubUC{statuses: []func() (dto.QueueStatusResponse, error){
		queued(2),
		resp("SOLD_OUT"),
	}}

	rr := get(newMux(uc, newWaker(), 10*time.Millisecond), streamPath)

	assert.Contains(t, rr.Body.String(), "event: sold_out", "ticker menjadi pengaman bila sinyal Pub/Sub terlewat")
}

func TestStream_ShouldSendErrorEvent_WhenStatusFailsMidStream(t *testing.T) {
	uc := &stubUC{statuses: []func() (dto.QueueStatusResponse, error){
		queued(2),
		fail(errors.New("redis putus")),
	}}
	w := newWaker()
	w.ch <- struct{}{}

	rr := get(newMux(uc, w, time.Hour), streamPath)

	body := rr.Body.String()
	assert.Contains(t, body, "event: error")
	assert.Contains(t, body, "gagal membaca status antrian")
	assert.NotContains(t, body, "redis putus", "detail internal tidak boleh bocor ke klien")
}

func TestStream_ShouldStop_WhenClientDisconnects(t *testing.T) {
	uc := &stubUC{statuses: []func() (dto.QueueStatusResponse, error){queued(9)}}
	w := newWaker()
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, streamPath, nil).WithContext(ctx)
	done := make(chan struct{})

	go func() {
		newMux(uc, w, time.Hour).ServeHTTP(httptest.NewRecorder(), req)
		close(done)
	}()
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler harus berhenti saat klien memutus koneksi")
	}
	select {
	case <-w.unsubscribed:
	case <-time.After(time.Second):
		t.Fatal("harus unsubscribe saat klien putus")
	}
}

// noFlushWriter = ResponseWriter tanpa http.Flusher.
type noFlushWriter struct {
	header http.Header
	code   int
	body   strings.Builder
}

func (n *noFlushWriter) Header() http.Header         { return n.header }
func (n *noFlushWriter) Write(b []byte) (int, error) { return n.body.Write(b) }
func (n *noFlushWriter) WriteHeader(code int)        { n.code = code }

func TestStream_ShouldFail_WhenWriterCannotFlush(t *testing.T) {
	uc := &stubUC{statuses: []func() (dto.QueueStatusResponse, error){queued(1)}}
	w := &noFlushWriter{header: http.Header{}}

	newMux(uc, newWaker(), time.Hour).ServeHTTP(w, httptest.NewRequest(http.MethodGet, streamPath, nil))

	assert.Equal(t, http.StatusInternalServerError, w.code)
	require.NotContains(t, w.header.Get("Content-Type"), "event-stream")
}
