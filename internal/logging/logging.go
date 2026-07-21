// Package logging defines the slog conventions shared by the server and
// its sibling tools: a TRACE level below slog's DEBUG, level-name parsing
// for the --log-level flag, and a handler hook that renders the custom
// level with a readable name.
//
// Level policy:
//   - INFO  — process and connection lifecycle an operator wants in
//     steady state: startup/config, listener bound, shutdown, and
//     connection open/close.
//   - DEBUG — state changes: attachment attach/detach and connection
//     state transitions.
//   - TRACE — per-operation detail: frames received, publishes, presence
//     ops, history queries. High volume; off by default.
package logging

import (
	"context"
	"fmt"
	"log/slog"
)

// LevelTrace is a custom slog level one step below slog.LevelDebug (-4),
// used for high-volume per-operation logs. slog has no built-in TRACE.
const LevelTrace = slog.Level(-8)

// LevelNames lists the accepted --log-level values, most to least
// verbose, for use in flag help and error messages.
const LevelNames = "trace, debug, info, warn, error"

// ParseLevel maps a --log-level name to its slog.Level. An unrecognised
// name is a startup error rather than a silent default.
func ParseLevel(name string) (slog.Level, error) {
	switch name {
	case "trace":
		return LevelTrace, nil
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q (valid: %s)", name, LevelNames)
	}
}

// ReplaceAttr is a slog.HandlerOptions.ReplaceAttr hook that renders
// LevelTrace as "TRACE"; slog would otherwise print it as "DEBUG-4".
// Standard levels are left untouched.
func ReplaceAttr(_ []string, a slog.Attr) slog.Attr {
	if a.Key == slog.LevelKey {
		if lvl, ok := a.Value.Any().(slog.Level); ok && lvl == LevelTrace {
			a.Value = slog.StringValue("TRACE")
		}
	}
	return a
}

// Logger embeds *slog.Logger so Debug/Info/Warn/Error/Log/Enabled are used
// unchanged, and adds Trace for LevelTrace — slog.Logger has no such method.
// With and WithGroup are overridden purely to return *Logger instead of
// *slog.Logger, so a scoped logger keeps Trace.
type Logger struct {
	*slog.Logger
}

// New builds a Logger backed by h, mirroring slog.New.
func New(h slog.Handler) *Logger {
	return &Logger{slog.New(h)}
}

// Default returns a Logger wrapping slog.Default().
func Default() *Logger {
	return &Logger{slog.Default()}
}

// Trace logs at LevelTrace.
func (l *Logger) Trace(msg string, args ...any) {
	l.Log(context.Background(), LevelTrace, msg, args...)
}

// With returns a Logger whose attrs are l's plus args, per slog.Logger.With.
func (l *Logger) With(args ...any) *Logger {
	return &Logger{l.Logger.With(args...)}
}

// WithGroup returns a Logger that starts a group, per slog.Logger.WithGroup.
func (l *Logger) WithGroup(name string) *Logger {
	return &Logger{l.Logger.WithGroup(name)}
}
