package realtime

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/memory"
)

// TestSpansEmittedWhenTracingEnabled injects a recording tracer (backed
// by an in-memory exporter) into the realtime Server, drives a WebSocket
// connect + publish, and asserts the connection-lifecycle and publish
// spans are exported (DESIGN.md §10, TASK-41 AC#3).
func TestSpansEmittedWhenTracingEnabled(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	manager := core.NewManager(memory.New(memory.Options{}))
	rt := NewServer([]auth.APIKey{parsed}, manager, time.Hour, slog.New(slog.DiscardHandler), nil, tp.Tracer("test"))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rt.HandleWebSocket)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	ws := dial(t, srv, "json")
	if f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionConnected {
		t.Fatalf("first frame = %v, want CONNECTED", f.Action)
	}

	// Publish one message; reading its ACK proves the publish span's work
	// completed (the span ends when the worker closure returns).
	writeProto(t, ws, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "traced",
		MsgSerial: 1,
		Messages:  []*protocol.Message{{Name: "n", Data: "d"}},
	})
	if f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionAck {
		t.Fatalf("frame = %v, want ACK", f.Action)
	}

	// Closing the socket ends the connection loop, which ends the
	// connection-lifecycle span.
	_ = ws.Close()

	if !waitForSpans(exporter, 2*time.Second, "publish", "ws.connection") {
		var got []string
		for _, s := range exporter.GetSpans() {
			got = append(got, s.Name)
		}
		t.Fatalf("did not observe expected spans; got %v", got)
	}
}

// waitForSpans polls the exporter until every wanted span name has been
// recorded or the timeout elapses.
func waitForSpans(exporter *tracetest.InMemoryExporter, timeout time.Duration, want ...string) bool {
	deadline := time.Now().Add(timeout)
	for {
		seen := make(map[string]bool)
		for _, s := range exporter.GetSpans() {
			seen[s.Name] = true
		}
		all := true
		for _, w := range want {
			if !seen[w] {
				all = false
				break
			}
		}
		if all {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func writeProto(t *testing.T, ws *websocket.Conn, m *protocol.ProtocolMessage) {
	t.Helper()
	data, err := protocol.Marshal(m, protocol.FormatJSON)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := ws.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("ws write: %v", err)
	}
}
