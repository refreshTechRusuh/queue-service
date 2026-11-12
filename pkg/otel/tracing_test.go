package otel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

// Jalur "aktif" mengekspor ke GCP Cloud Trace (butuh kredensial) sehingga tidak diuji di sini.

func TestSetupTracerProvider_ShouldBeNoop_WhenDisabled(t *testing.T) {
	t.Setenv("OTEL_ENABLED", "false")
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })

	shutdown, err := SetupTracerProvider(context.Background(), "svc-uji", "0.0.1")

	require.NoError(t, err)
	require.NotNil(t, shutdown)
	assert.NoError(t, shutdown(context.Background()))
	_, span := Tracer("uji").Start(context.Background(), "op")
	assert.False(t, span.SpanContext().IsValid(), "noop: span tidak punya trace ID")
	span.End()
}

func TestSetupTracerProvider_Disabled_ShouldStillPropagateTraceContext(t *testing.T) {
	t.Setenv("OTEL_ENABLED", "false")
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })

	_, err := SetupTracerProvider(context.Background(), "svc-uji", "0.0.1")
	require.NoError(t, err)

	assert.Contains(t, otel.GetTextMapPropagator().Fields(), "traceparent",
		"walau tracing dimatikan, service tetap meneruskan traceparent ke hilir")
}

func TestSampleRatio(t *testing.T) {
	cases := map[string]float64{
		"":      1.0, // default: sampel semua
		"0.25":  0.25,
		"0":     0,
		"1":     1.0,
		"-3":    0, // di-clamp
		"7":     1.0,
		"abc":   1.0, // tak terbaca → default
		"0.5xx": 0.5, // Sscanf membaca awalan angka
	}
	for in, want := range cases {
		t.Run("OTEL_SAMPLE_RATIO="+in, func(t *testing.T) {
			t.Setenv("OTEL_SAMPLE_RATIO", in)
			assert.InDelta(t, want, sampleRatio(), 1e-9)
		})
	}
}

func TestTracer_ShouldReturnUsableTracer(t *testing.T) {
	assert.NotNil(t, Tracer("uji"))
}
