// Package tracing wires optional OpenTelemetry trace export, configured
// entirely through the standard OTEL_* environment variables (DESIGN.md
// §10). It is off by default: with no OTEL_* configuration present no
// exporter is built, no background goroutine is started, and the global
// tracer provider is left as the SDK's no-op, so instrumentation adds no
// export overhead.
package tracing

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// instrumentationScope names the tracer used for the spans this server
// emits directly (WebSocket connection lifecycle and the publish path);
// otelhttp emits its own HTTP server spans.
const instrumentationScope = "github.com/ably/ably-server"

// defaultServiceName is the resource service.name used when
// OTEL_SERVICE_NAME is not set.
const defaultServiceName = "ably-server"

// Provider is the result of Setup: a Tracer for the server to emit spans
// with, whether tracing is Enabled, and a Shutdown that flushes and stops
// the exporter (a no-op when disabled).
type Provider struct {
	// Tracer emits the server's spans. When tracing is disabled it is a
	// no-op tracer; callers should still prefer to skip span creation
	// entirely on the hot path (pass nil where a nil-check guards it) so
	// disabled tracing costs nothing.
	Tracer trace.Tracer

	// Enabled reports whether an exporter was configured and started.
	Enabled bool

	shutdown func(context.Context) error
}

// Shutdown flushes any buffered spans and stops the exporter. Safe to
// call when tracing is disabled (it is then a no-op).
func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil || p.shutdown == nil {
		return nil
	}
	return p.shutdown(ctx)
}

// Setup inspects the OTEL_* environment (via getenv) and, when tracing is
// enabled, builds an OTLP/HTTP trace exporter, installs a batching
// TracerProvider as the global provider, and sets the W3C propagators.
// When tracing is disabled it returns a Provider wrapping a no-op tracer
// and does not touch global state or start any goroutine.
//
// The exporter is the OTLP/HTTP one (otlptracehttp), chosen over
// contrib/autoexport as the smallest dependency set that still honours
// the standard OTEL_EXPORTER_OTLP_* variables — it reads the endpoint,
// headers, protocol, and TLS settings from the environment itself.
func Setup(ctx context.Context, getenv func(string) string) (*Provider, error) {
	if !enabled(getenv) {
		return &Provider{Tracer: noop.NewTracerProvider().Tracer(instrumentationScope)}, nil
	}

	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}

	// service.name is seeded to a default, then overridden by
	// WithFromEnv so OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES win when
	// present (later options override earlier ones).
	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithAttributes(attribute.String("service.name", defaultServiceName)),
		resource.WithFromEnv(),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &Provider{
		Tracer:   tp.Tracer(instrumentationScope),
		Enabled:  true,
		shutdown: tp.Shutdown,
	}, nil
}

// enabled reports whether the OTEL_* environment asks for trace export.
// Tracing is off unless an OTLP endpoint is configured or a traces
// exporter is explicitly selected, and is always off when the SDK is
// disabled or the traces exporter is "none".
func enabled(getenv func(string) string) bool {
	if strings.EqualFold(getenv("OTEL_SDK_DISABLED"), "true") {
		return false
	}
	if exporter := getenv("OTEL_TRACES_EXPORTER"); exporter != "" {
		return !strings.EqualFold(exporter, "none")
	}
	return getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" ||
		getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
}
