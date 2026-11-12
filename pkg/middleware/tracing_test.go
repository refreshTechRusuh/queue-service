package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// useRecorder memasang tracer provider yang merekam span di memori selama satu test.
func useRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
		_ = tp.Shutdown(t.Context())
	})
	return rec
}

func serveTraced(h http.Handler, path string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

var okHandler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

func TestOtelMiddleware_ShouldCreateOneSpanPerRequest(t *testing.T) {
	rec := useRecorder(t)

	rr := serveTraced(OtelMiddleware("svc-uji")(okHandler), "/api/x", nil)

	require.Equal(t, http.StatusOK, rr.Code)
	spans := rec.Ended()
	require.Len(t, spans, 1)
	assert.NotEmpty(t, spans[0].Name())
}

func TestOtelMiddleware_ShouldContinueTraceFromIncomingTraceparent(t *testing.T) {
	rec := useRecorder(t)
	const parent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

	serveTraced(OtelMiddleware("svc-uji")(okHandler), "/api/x", map[string]string{"traceparent": parent})

	spans := rec.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "0af7651916cd43dd8448eb211c80319c", spans[0].SpanContext().TraceID().String(),
		"trace ID harus mengikuti pemanggil agar trace tersambung lintas service")
	assert.Equal(t, "b7ad6b7169203331", spans[0].Parent().SpanID().String())
}

func TestOtelMiddleware_ShouldSkipConfiguredSuffixes(t *testing.T) {
	rec := useRecorder(t)
	h := OtelMiddleware("svc-uji", "/sys/ping", "/stream")(okHandler)

	serveTraced(h, "/sys/ping", nil)
	serveTraced(h, "/api/queue/v1/event/e/q_t/stream", nil)
	serveTraced(h, "/api/queue/v1/event/e/join", nil)

	spans := rec.Ended()
	require.Len(t, spans, 1, "health check & SSE tidak boleh membuat span")
}

func TestOtelMiddleware_ShouldStillServeSkippedPaths(t *testing.T) {
	useRecorder(t)
	h := OtelMiddleware("svc-uji", "/sys/ping")(okHandler)

	assert.Equal(t, http.StatusOK, serveTraced(h, "/sys/ping", nil).Code, "path yang di-skip tetap dilayani")
}

func TestOtelMiddleware_ShouldPreserveFlusher_ForSSE(t *testing.T) {
	useRecorder(t)
	var flushable bool
	sse := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, flushable = w.(http.Flusher)
	})

	serveTraced(OtelMiddleware("svc-uji")(sse), "/api/x", nil)

	assert.True(t, flushable, "pembungkus tracing tidak boleh menghilangkan http.Flusher (SSE akan gagal)")
}

func TestOtelMiddleware_ShouldNotPanic_WhenNoTracerProviderConfigured(t *testing.T) {
	assert.NotPanics(t, func() {
		serveTraced(OtelMiddleware("svc-uji")(okHandler), "/api/x", nil)
	})
}
