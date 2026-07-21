package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestParseLevel(t *testing.T) {
	cases := []struct {
		name string
		want slog.Level
	}{
		{"trace", LevelTrace},
		{"debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"error", slog.LevelError},
	}
	for _, tc := range cases {
		got, err := ParseLevel(tc.name)
		if err != nil {
			t.Errorf("ParseLevel(%q) error = %v", tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseLevel(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestParseLevelUnknown(t *testing.T) {
	if _, err := ParseLevel("verbose"); err == nil {
		t.Error("ParseLevel(\"verbose\") error = nil, want an error for an unrecognised level")
	}
}

// TestTraceRendering checks that a logger built at TRACE emits trace lines
// and that ReplaceAttr renders the custom level as "TRACE" rather than the
// slog default "DEBUG-4".
func TestTraceRendering(t *testing.T) {
	var buf bytes.Buffer
	logger := New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level:       LevelTrace,
		ReplaceAttr: ReplaceAttr,
	}))
	logger.Trace("frame received", "action", "MESSAGE")

	out := buf.String()
	if !strings.Contains(out, "level=TRACE") {
		t.Errorf("output %q missing level=TRACE", out)
	}
	if strings.Contains(out, "DEBUG-4") {
		t.Errorf("output %q rendered the raw DEBUG-4 level", out)
	}
	if !strings.Contains(out, "frame received") || !strings.Contains(out, "action=MESSAGE") {
		t.Errorf("output %q missing message or attributes", out)
	}
}

// TestTraceBelowDebug confirms TRACE is suppressed when the handler is set
// to DEBUG, so trace stays off unless explicitly requested.
func TestTraceBelowDebug(t *testing.T) {
	var buf bytes.Buffer
	logger := New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level:       slog.LevelDebug,
		ReplaceAttr: ReplaceAttr,
	}))
	logger.Trace("frame received")
	if buf.Len() != 0 {
		t.Errorf("TRACE line emitted at DEBUG level: %q", buf.String())
	}
}

// TestWithPreservesTrace confirms a scoped logger built via With keeps a
// working Trace method rather than losing it to the promoted slog.Logger.With.
func TestWithPreservesTrace(t *testing.T) {
	var buf bytes.Buffer
	logger := New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level:       LevelTrace,
		ReplaceAttr: ReplaceAttr,
	}))
	scoped := logger.With("connId", "abc123")
	scoped.Trace("frame received")

	out := buf.String()
	if !strings.Contains(out, "level=TRACE") || !strings.Contains(out, "connId=abc123") {
		t.Errorf("output %q missing level=TRACE or connId=abc123", out)
	}
}
