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
