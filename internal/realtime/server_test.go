package realtime

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/protocol"
)

const testKey = "app.key:secret"

// newTestServer constructs an httptest.Server wrapping our realtime
// Server with a known API key.
func newTestServer(t *testing.T, hb time.Duration) *httptest.Server {
	t.Helper()
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	cfg := Config{
		Key:               parsed,
		HeartbeatInterval: hb,
	}
	srv := httptest.NewServer(NewServer(cfg))
	t.Cleanup(srv.Close)
	return srv
}

// dial connects a WebSocket client to srv with the test key included as
// `?key=` and the requested format.
func dial(t *testing.T, srv *httptest.Server, format string) *websocket.Conn {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	u.Scheme = "ws"
	q := u.Query()
	q.Set("key", testKey)
	if format != "" {
		q.Set("format", format)
	}
	u.RawQuery = q.Encode()

	ws, _, err := websocket.DefaultDialer.DialContext(context.Background(), u.String(), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = ws.Close() })
	return ws
}

// readFrame reads one ProtocolMessage from ws within the deadline.
func readFrame(t *testing.T, ws *websocket.Conn, format protocol.Format, within time.Duration) *protocol.ProtocolMessage {
	t.Helper()
	if err := ws.SetReadDeadline(time.Now().Add(within)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	_, data, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m protocol.ProtocolMessage
	if err := protocol.Unmarshal(data, format, &m); err != nil {
		t.Fatalf("unmarshal (%s, %q): %v", format, string(data), err)
	}
	return &m
}

func TestConnectedIsFirstFrame(t *testing.T) {
	srv := newTestServer(t, time.Hour)

	for _, format := range []string{"json", "msgpack"} {
		t.Run(format, func(t *testing.T) {
			ws := dial(t, srv, format)
			f, _ := protocol.FormatFromQuery(format)

			msg := readFrame(t, ws, f, 2*time.Second)
			if msg.Action != protocol.ActionConnected {
				t.Fatalf("first frame Action = %v, want CONNECTED", msg.Action)
			}
			if len(msg.ConnectionID) != 12 {
				t.Fatalf("ConnectionID length = %d, want 12 (got %q)", len(msg.ConnectionID), msg.ConnectionID)
			}
		})
	}
}

func TestPeriodicHeartbeat(t *testing.T) {
	srv := newTestServer(t, 30*time.Millisecond)

	ws := dial(t, srv, "")

	// Drain the CONNECTED frame.
	first := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if first.Action != protocol.ActionConnected {
		t.Fatalf("first frame Action = %v, want CONNECTED", first.Action)
	}

	// Expect at least two HEARTBEATs within ~5x the interval.
	deadline := time.Now().Add(500 * time.Millisecond)
	got := 0
	for time.Now().Before(deadline) && got < 2 {
		msg := readFrame(t, ws, protocol.FormatJSON, 200*time.Millisecond)
		if msg.Action != protocol.ActionHeartbeat {
			t.Fatalf("unexpected frame: %v", msg.Action)
		}
		got++
	}
	if got < 2 {
		t.Fatalf("got %d heartbeats within deadline, want >= 2", got)
	}
}

func TestUnsupportedFormatRejectedAtUpgrade(t *testing.T) {
	srv := newTestServer(t, time.Hour)

	u := strings.Replace(srv.URL, "http://", "ws://", 1) + "?key=" + testKey + "&format=protobuf"
	_, resp, err := websocket.DefaultDialer.DialContext(context.Background(), u, nil)
	if err == nil {
		t.Fatal("dial succeeded; expected upgrade rejection")
	}
	if resp == nil {
		t.Fatalf("nil response with err: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "unsupported format") {
		t.Fatalf("body = %q, want substring %q", body, "unsupported format")
	}
}

func TestInboundFrameDoesNotCrashConnection(t *testing.T) {
	srv := newTestServer(t, time.Hour)

	ws := dial(t, srv, "")
	first := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if first.Action != protocol.ActionConnected {
		t.Fatalf("first frame Action = %v, want CONNECTED", first.Action)
	}

	// Send a HEARTBEAT inbound; the server logs it but does not respond.
	hb := &protocol.ProtocolMessage{Action: protocol.ActionHeartbeat}
	data, err := protocol.Marshal(hb, protocol.FormatJSON)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := ws.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Connection should still be alive — closing cleanly should not error.
	if err := ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")); err != nil {
		t.Fatalf("close write: %v", err)
	}
}

func TestUpgradeRejectedWithoutCredentials(t *testing.T) {
	srv := newTestServer(t, time.Hour)

	wsURL := strings.Replace(srv.URL, "http://", "ws://", 1)
	_, resp, err := websocket.DefaultDialer.DialContext(context.Background(), wsURL, nil)
	if err == nil {
		t.Fatal("dial succeeded; expected 401")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %v, want 401", resp)
	}
	if got := resp.Header.Get("WWW-Authenticate"); !strings.Contains(got, "Basic") {
		t.Errorf("WWW-Authenticate = %q, want Basic challenge", got)
	}
}

func TestUpgradeRejectedWithWrongKey(t *testing.T) {
	srv := newTestServer(t, time.Hour)

	wsURL := strings.Replace(srv.URL, "http://", "ws://", 1) + "?key=app.key:wrong"
	_, resp, err := websocket.DefaultDialer.DialContext(context.Background(), wsURL, nil)
	if err == nil {
		t.Fatal("dial succeeded; expected 401")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %v, want 401", resp)
	}
}

func TestUpgradeAcceptsBasicAuth(t *testing.T) {
	srv := newTestServer(t, time.Hour)

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	u.Scheme = "ws"

	headers := http.Header{}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("app.key", "secret")
	headers.Set("Authorization", req.Header.Get("Authorization"))

	ws, _, err := websocket.DefaultDialer.DialContext(context.Background(), u.String(), headers)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer ws.Close()

	msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if msg.Action != protocol.ActionConnected {
		t.Fatalf("first frame Action = %v, want CONNECTED", msg.Action)
	}
}
