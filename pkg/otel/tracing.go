// Package otel menyediakan setup OpenTelemetry TracerProvider untuk GCP Cloud Trace.
// Di Cloud Run, resource attributes (project-id, service name, revision) dideteksi
// otomatis oleh GCP detector dari metadata server.
// Di lokal, set OTEL_ENABLED=false untuk skip export ke GCP.
package otel

import (
	"context"
	"fmt"
	"os"

	texporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// SetupTracerProvider inisialisasi global TracerProvider dan TextMapPropagator.
//
// Di Cloud Run: export ke GCP Cloud Trace menggunakan credential dari metadata server (WIF).
// Di lokal: set OTEL_ENABLED=false agar tidak connect ke GCP, TracerProvider diganti noop.
//
// Mengembalikan shutdown func yang harus dipanggil saat graceful shutdown.
func SetupTracerProvider(ctx context.Context, serviceName, version string) (func(context.Context) error, error) {
	if os.Getenv("OTEL_ENABLED") == "false" {
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
		return func(_ context.Context) error { return nil }, nil
	}

	// Exporter ke GCP Cloud Trace.
	exporter, err := texporter.New()
	if err != nil {
		return nil, fmt.Errorf("otel: create cloud trace exporter: %w", err)
	}

	// Resource: GCP detector otomatis mengisi project-id, service name, revision, instance-id
	// saat berjalan di Cloud Run. Di lokal hanya mengisi service.name dari semconv.
	res, err := resource.New(ctx,
		resource.WithDetectors(gcp.NewDetector()),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(version),
		),
		resource.WithTelemetrySDK(),
	)
	if err != nil {
		// Resource detection error di lokal (non-GCP) adalah wajar; lanjut dengan resource minimal.
		res = resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(version),
		)
	}

	// Sampler: AlwaysSample di development/staging.
	// Di production set OTEL_SAMPLE_RATIO env untuk ratio sampling.
	sampler := sdktrace.AlwaysSample()
	if ratio := sampleRatio(); ratio < 1.0 {
		sampler = sdktrace.TraceIDRatioBased(ratio)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	// Set global TracerProvider dan propagator (W3C TraceContext + Baggage).
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// Tracer mengembalikan tracer dengan nama package pemanggil.
// Shortcut agar tidak perlu import otel di setiap file.
func Tracer(name string) trace.Tracer {
	return otel.GetTracerProvider().Tracer(name)
}

// sampleRatio membaca OTEL_SAMPLE_RATIO dari env, default 1.0.
func sampleRatio() float64 {
	val := os.Getenv("OTEL_SAMPLE_RATIO")
	if val == "" {
		return 1.0
	}
	var r float64
	if _, err := fmt.Sscanf(val, "%f", &r); err != nil {
		return 1.0
	}
	if r < 0 {
		return 0
	}
	if r > 1 {
		return 1.0
	}
	return r
}
