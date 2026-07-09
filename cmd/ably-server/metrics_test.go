package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/protocol"
)

// bootMemoryServer boots ably-server in memory mode on a free port and
// returns its "host:port". Torn down on t.Cleanup. Unlike the
// integration harness it needs no Postgres, so it runs in the default
// build.
func bootMemoryServer(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan net.Addr, 1)
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, runOpts{
			Args: []string{
				"--api-key=app.key:secret",
				"--mode=memory",
				"--listen=127.0.0.1:0",
				"--log-level=error",
			},
			Getenv: func(string) string { return "" },
			Out:    io.Discard,
			Ready:  ready,
		})
	}()
	var addr net.Addr
	select {
	case addr = <-ready:
	case code := <-done:
		t.Fatalf("server exited before ready (code=%d)", code)
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("server did not become ready within 10s")
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("server did not shut down within 10s")
		}
	})
	return addr.String()
}

// TestMetricsEndpoint drives a WebSocket connect + attach, a REST
// publish, and then scrapes GET /metrics, asserting the process-wide
// series enumerated in DESIGN.md §10 are present with plausible values.
func TestMetricsEndpoint(t *testing.T) {
	addr := bootMemoryServer(t)

	// WebSocket connect + attach so a subscriber is live for the publish.
	wsURL := (&url.URL{Scheme: "ws", Host: addr, Path: "/"}).String() +
		"?key=app.key:secret&v=2&format=json"
	ws, _, err := websocket.DefaultDialer.DialContext(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer ws.Close()

	if f := readProto(t, ws); f.Action != protocol.ActionConnected {
		t.Fatalf("first frame = %v, want CONNECTED", f.Action)
	}
	writeProto(t, ws, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "metrics-ch"})
	if f := readProto(t, ws); f.Action != protocol.ActionAttached {
		t.Fatalf("frame = %v, want ATTACHED", f.Action)
	}

	// REST publish to the attached channel.
	pub, _ := http.NewRequest(http.MethodPost,
		(&url.URL{Scheme: "http", Host: addr, Path: "/channels/metrics-ch/messages"}).String(),
		strings.NewReader(`{"name":"x","data":"y"}`))
	pub.Header.Set("Content-Type", "application/json")
	pub.SetBasicAuth("app.key", "secret")
	resp, err := http.DefaultClient.Do(pub)
	if err != nil {
		t.Fatalf("REST publish: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("REST publish status = %d, want 201", resp.StatusCode)
	}

	// The subscriber should receive the delivered MESSAGE frame; reading
	// it guarantees the delivered counter has been incremented before we
	// scrape.
	if f := readProto(t, ws); f.Action != protocol.ActionMessage {
		t.Fatalf("frame = %v, want MESSAGE", f.Action)
	}

	body := scrapeMetrics(t, addr)

	// Counters/gauges: exact expected minimums.
	assertMetricAtLeast(t, body, "ably_connections_opened_total", 1)
	assertMetricAtLeast(t, body, "ably_connections_open", 1)
	assertMetricAtLeast(t, body, "ably_attachments_total", 1)
	assertMetricAtLeast(t, body, "ably_messages_published_total", 1)
	assertMetricAtLeast(t, body, "ably_messages_delivered_total", 1)
	assertMetricAtLeast(t, body, "ably_publish_latency_seconds_count", 1)

	// Histograms always export their _count series even with zero
	// observations; the connection is still open, so just assert presence.
	if !strings.Contains(body, "ably_connection_lifetime_seconds_count") {
		t.Errorf("missing ably_connection_lifetime_seconds series")
	}

	// The labelled HTTP counter must carry the publish request.
	if !strings.Contains(body, `ably_http_requests_total{method="POST",route="/channels/{name}/messages",status="201"}`) {
		t.Errorf("missing ably_http_requests_total series for the REST publish; body:\n%s", body)
	}
}

func scrapeMetrics(t *testing.T, addr string) string {
	t.Helper()
	resp, err := http.Get((&url.URL{Scheme: "http", Host: addr, Path: "/metrics"}).String())
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want 200", resp.StatusCode)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /metrics: %v", err)
	}
	return string(b)
}

// assertMetricAtLeast finds the unlabelled sample line for name and
// asserts its value is >= min.
func assertMetricAtLeast(t *testing.T, body, name string, min float64) {
	t.Helper()
	for line := range strings.SplitSeq(body, "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, " ")
		if !ok || key != name {
			continue
		}
		got, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
		if err != nil {
			t.Fatalf("parse %s value %q: %v", name, val, err)
		}
		if got < min {
			t.Errorf("%s = %v, want >= %v", name, got, min)
		}
		return
	}
	t.Errorf("metric %q not found in /metrics output:\n%s", name, body)
}

func readProto(t *testing.T, ws *websocket.Conn) *protocol.ProtocolMessage {
	t.Helper()
	_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, data, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("ws read: %v", err)
	}
	var m protocol.ProtocolMessage
	if err := protocol.Unmarshal(data, protocol.FormatJSON, &m); err != nil {
		t.Fatalf("unmarshal %q: %v", data, err)
	}
	return &m
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
