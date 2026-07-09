package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// emptyEnv is a getenv stub that returns "" for every key.
func emptyEnv(string) string { return "" }

// envWith returns a getenv stub that returns vals[k] for known keys
// and "" otherwise.
func envWith(vals map[string]string) func(string) string {
	return func(k string) string { return vals[k] }
}

func TestRunRejectsMissingKey(t *testing.T) {
	var out bytes.Buffer
	code := run(context.Background(), runOpts{
		Args:   nil,
		Getenv: emptyEnv,
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), "api key is required") {
		t.Errorf("output = %q, want substring %q", out.String(), "api key is required")
	}
}

func TestRunRejectsMalformedKeyFromFlag(t *testing.T) {
	var out bytes.Buffer
	code := run(context.Background(), runOpts{
		Args:   []string{"--api-key=bogus"},
		Getenv: emptyEnv,
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), "invalid api key") {
		t.Errorf("output = %q, want substring %q", out.String(), "invalid api key")
	}
}

func TestRunFallsBackToEnv(t *testing.T) {
	// Flag is absent; the env value must be picked up. We supply a
	// malformed env value so the parse error proves the env was read
	// — without starting the server.
	var out bytes.Buffer
	code := run(context.Background(), runOpts{
		Args:   nil,
		Getenv: envWith(map[string]string{apiKeyEnv: "bogus"}),
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), "invalid api key") {
		t.Errorf("output = %q, want substring %q", out.String(), "invalid api key")
	}
}

func TestRunRejectsUnknownLogFormat(t *testing.T) {
	var out bytes.Buffer
	code := run(context.Background(), runOpts{
		Args:   []string{"--log-format=xml"},
		Getenv: emptyEnv,
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), `unknown --log-format "xml"`) {
		t.Errorf("output = %q, want substring about unknown --log-format", out.String())
	}
}

func TestRunLogFormatEnvFallback(t *testing.T) {
	// No --log-format flag; the env value must be picked up. An
	// invalid env value surfaces the same startup error as an invalid
	// flag value would, proving the env was read.
	var out bytes.Buffer
	code := run(context.Background(), runOpts{
		Args:   nil,
		Getenv: envWith(map[string]string{logFormatEnv: "xml"}),
		Out:    &out,
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), `unknown --log-format "xml"`) {
		t.Errorf("output = %q, want substring about unknown --log-format", out.String())
	}
}

func TestNewLoggerSelectsHandler(t *testing.T) {
	var out bytes.Buffer
	logger, err := newLogger("info", "json", &out)
	if err != nil {
		t.Fatalf("newLogger(json) error: %v", err)
	}
	logger.Info("hello")
	if !strings.HasPrefix(strings.TrimSpace(out.String()), "{") {
		t.Errorf("json output = %q, want a JSON object", out.String())
	}

	out.Reset()
	logger, err = newLogger("info", "text", &out)
	if err != nil {
		t.Fatalf("newLogger(text) error: %v", err)
	}
	logger.Info("hello")
	if strings.HasPrefix(strings.TrimSpace(out.String()), "{") {
		t.Errorf("text output = %q, want slog's key=value form", out.String())
	}

	if _, err := newLogger("info", "yaml", &out); err == nil {
		t.Error("newLogger(yaml) error = nil, want an error for an unrecognised format")
	}
}
