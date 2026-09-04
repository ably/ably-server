package logging

import (
	"context"
	"log/slog"

	protocollog "github.com/ably/server-protocol/go/logging"
	"github.com/ably/server-protocol/go/scope"
)

// Protocol returns the logger as the shared protocol code writes to it.
//
// That code logs at levels this server does not have. It separates a fault
// the server caused from one the client caused, because a rise in the latter
// means a client is misbehaving rather than that the server is unwell, and it
// separates a fault worth paging someone about from one worth only logging.
// Neither distinction is worth a level of its own here, so both are folded
// onto slog levels of the same severity and marked with an attribute, which
// is enough to grep or route on later.
func (l *Logger) Protocol() protocollog.Logger { return protocolLogger{l} }

type protocolLogger struct{ log *Logger }

var _ protocollog.Logger = protocolLogger{}

func (p protocolLogger) Trace(msg string, fields ...any) { p.log.Trace(msg, fields...) }
func (p protocolLogger) Debug(msg string, fields ...any) { p.log.Debug(msg, fields...) }
func (p protocolLogger) Info(msg string, fields ...any)  { p.log.Info(msg, fields...) }
func (p protocolLogger) Warn(msg string, fields ...any)  { p.log.Warn(msg, fields...) }
func (p protocolLogger) Error(msg string, fields ...any) { p.log.Error(msg, fields...) }

// ClientWarn and ClientErr are caused by the client, so they sit a step below
// their server-caused counterparts: a rejected request is normal traffic.
func (p protocolLogger) ClientWarn(msg string, fields ...any) {
	p.log.Debug(msg, append(fields, "client", true)...)
}

func (p protocolLogger) ClientErr(msg string, fields ...any) {
	p.log.Info(msg, append(fields, "client", true)...)
}

// Reportable would go to an error tracker on a server that has one.
func (p protocolLogger) Reportable(msg string, fields ...any) {
	p.log.Error(msg, append(fields, "reportable", true)...)
}

func (p protocolLogger) Log(lvl protocollog.Level, msg string, fields ...any) {
	switch lvl {
	case protocollog.Trace:
		p.Trace(msg, fields...)
	case protocollog.Debug:
		p.Debug(msg, fields...)
	case protocollog.ClientWarn:
		p.ClientWarn(msg, fields...)
	case protocollog.Info:
		p.Info(msg, fields...)
	case protocollog.ClientErr:
		p.ClientErr(msg, fields...)
	case protocollog.Warn:
		p.Warn(msg, fields...)
	case protocollog.Error:
		p.Error(msg, fields...)
	case protocollog.Reportable:
		p.Reportable(msg, fields...)
	}
}

func (p protocolLogger) Enabled(lvl protocollog.Level) bool {
	return p.log.Logger.Enabled(context.Background(), slogLevel(lvl))
}

func (p protocolLogger) With(fields ...any) protocollog.Logger {
	return protocolLogger{p.log.With(fields...)}
}

// WithScopes records which app, channel or connection a line is about. This
// server has one app and names its channels and connections in fields
// already, so the scopes go on as ordinary attributes keyed by their type.
func (p protocolLogger) WithScopes(scopes ...scope.ID) protocollog.Logger {
	fields := make([]any, 0, len(scopes)*2)
	for _, s := range scopes {
		fields = append(fields, string(s.Type), s.Item)
	}
	return protocolLogger{p.log.With(fields...)}
}

// WithContext takes no identifiers off the context: this server's traces are
// carried by the otel span, and its handler does not read them.
func (p protocolLogger) WithContext(_ context.Context, fields ...any) protocollog.Logger {
	return protocolLogger{p.log.With(fields...)}
}

// slogLevel is the severity this server logs a protocol level at. The
// client-caused levels drop a step, as above.
func slogLevel(lvl protocollog.Level) slog.Level {
	switch lvl {
	case protocollog.Trace:
		return LevelTrace
	case protocollog.Debug, protocollog.ClientWarn:
		return slog.LevelDebug
	case protocollog.Info, protocollog.ClientErr:
		return slog.LevelInfo
	case protocollog.Warn:
		return slog.LevelWarn
	default:
		return slog.LevelError
	}
}
