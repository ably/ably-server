package tracing

import (
	"context"
	"testing"
	"time"
)

func TestEnabled(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want bool
	}{
		{"no env", nil, false},
		{"otlp endpoint set", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318"}, true},
		{"otlp traces endpoint set", map[string]string{"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://localhost:4318/v1/traces"}, true},
		{"traces exporter otlp", map[string]string{"OTEL_TRACES_EXPORTER": "otlp"}, true},
		{"traces exporter none", map[string]string{"OTEL_TRACES_EXPORTER": "none"}, false},
		{"sdk disabled overrides endpoint", map[string]string{
			"OTEL_SDK_DISABLED":           "true",
			"OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := enabled(getenvFrom(tc.env)); got != tc.want {
				t.Errorf("enabled(%v) = %v, want %v", tc.env, got, tc.want)
			}
		})
	}
}

// TestSetupDisabledByDefault asserts the zero-config path: no exporter,
// a no-op tracer whose spans do not record, and a no-op Shutdown.
func TestSetupDisabledByDefault(t *testing.T) {
	p, err := Setup(context.Background(), getenvFrom(nil))
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if p.Enabled {
		t.Fatal("Enabled = true, want false with no OTEL_* env")
	}
	_, span := p.Tracer.Start(context.Background(), "x")
	if span.IsRecording() {
		t.Error("span from disabled tracer is recording; want no-op")
	}
	span.End()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown on disabled provider: %v", err)
	}
}

// TestSetupEnabled asserts that a standard OTEL_* endpoint switches
// export on: the returned tracer records spans, and Shutdown stops the
// exporter cleanly. The endpoint need not be reachable — the batch
// exporter connects lazily, so no span is actually delivered here.
func TestSetupEnabled(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")
	p, err := Setup(context.Background(), getenvFrom(map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318",
	}))
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if !p.Enabled {
		t.Fatal("Enabled = false, want true with OTEL_EXPORTER_OTLP_ENDPOINT set")
	}
	_, span := p.Tracer.Start(context.Background(), "x")
	if !span.IsRecording() {
		t.Error("span from enabled tracer is not recording")
	}
	span.End()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

func getenvFrom(env map[string]string) func(string) string {
	return func(k string) string { return env[k] }
}
