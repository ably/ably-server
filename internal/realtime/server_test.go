package realtime

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/memory"
)

// testHarness gives tests a single handle for driving publishes
// through the same path the server uses.
type testHarness struct {
	manager *core.Manager
}

// publish runs a publish via core.Channel.Publish, which delegates to
// the storage backend (in-process for these tests). Fails the test
// on storage error.
func (h *testHarness) publish(t *testing.T, channel string, msgs ...*protocol.Message) {
	t.Helper()
	if _, _, err := h.manager.GetChannel(channel).Publish(context.Background(), msgs); err != nil {
		t.Fatalf("publish to %q: %v", channel, err)
	}
}

const testKey = "app.key:secret"

// newTestServer constructs an httptest.Server wrapping our realtime
// Server with a known API key. The returned testHarness wraps the
// Manager and Storage the server is wired with, so tests can publish
// to channels and observe attachment-driven forwarding via
// harness.publish.
func newTestServer(t *testing.T, hb time.Duration) (*httptest.Server, *testHarness) {
	t.Helper()
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	manager := core.NewManager(memory.New(memory.Options{}))
	rt := NewServer(parsed, manager, hb, slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rt.HandleWebSocket)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &testHarness{manager: manager}
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
	srv, _ := newTestServer(t, time.Hour)

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
	srv, _ := newTestServer(t, 30*time.Millisecond)

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
	srv, _ := newTestServer(t, time.Hour)

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
	srv, _ := newTestServer(t, time.Hour)

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
	srv, _ := newTestServer(t, time.Hour)

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
	srv, _ := newTestServer(t, time.Hour)

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
	srv, _ := newTestServer(t, time.Hour)

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

// sendFrame encodes and writes a ProtocolMessage to ws.
func sendFrame(t *testing.T, ws *websocket.Conn, format protocol.Format, msg *protocol.ProtocolMessage) {
	t.Helper()
	data, err := protocol.Marshal(msg, format)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	wsType := websocket.TextMessage
	if format == protocol.FormatMsgpack {
		wsType = websocket.BinaryMessage
	}
	if err := ws.WriteMessage(wsType, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// drainConnected reads and discards the initial CONNECTED frame.
func drainConnected(t *testing.T, ws *websocket.Conn) {
	t.Helper()
	first := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if first.Action != protocol.ActionConnected {
		t.Fatalf("first frame Action = %v, want CONNECTED", first.Action)
	}
}

func TestAttachReceivesAttachedAck(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "foo",
	})

	msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if msg.Action != protocol.ActionAttached {
		t.Fatalf("Action = %v, want ATTACHED", msg.Action)
	}
	if msg.Channel != "foo" {
		t.Errorf("Channel = %q, want %q", msg.Channel, "foo")
	}
	if msg.ChannelSerial != "" {
		t.Errorf("ChannelSerial = %q, want empty (fresh attach with no delivered messages)", msg.ChannelSerial)
	}
}

func TestAttachForwardsPublishedMessages(t *testing.T) {
	srv, h := newTestServer(t, time.Hour)
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "foo",
	})
	if msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); msg.Action != protocol.ActionAttached {
		t.Fatalf("expected ATTACHED, got %v", msg.Action)
	}

	h.publish(t, "foo", &protocol.Message{ID: "m1"})

	msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if msg.Action != protocol.ActionMessage {
		t.Fatalf("Action = %v, want MESSAGE", msg.Action)
	}
	if msg.Channel != "foo" {
		t.Errorf("Channel = %q, want %q", msg.Channel, "foo")
	}
	if msg.ChannelSerial == "" {
		t.Error("ChannelSerial is empty; want the delivered ChannelMessage's channelSerial")
	}
	if len(msg.Messages) != 1 {
		t.Fatalf("Messages length = %d, want 1", len(msg.Messages))
	}
	if msg.Messages[0].ID != "m1" {
		t.Errorf("Messages[0].ID = %q, want %q", msg.Messages[0].ID, "m1")
	}
	// Message.Serial = channelSerial + ":000" for a single-message publish.
	wantMsgSerial := msg.ChannelSerial + ":000"
	if msg.Messages[0].Serial != wantMsgSerial {
		t.Errorf("Messages[0].Serial = %q, want %q", msg.Messages[0].Serial, wantMsgSerial)
	}
}

func TestAttachIsIdempotentPerChannel(t *testing.T) {
	srv, h := newTestServer(t, time.Hour)
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	// Two ATTACHes for the same channel should not start two attachments;
	// only one ATTACHED is expected, and a subsequent publish produces one
	// MESSAGE.
	for range 2 {
		sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
			Action:  protocol.ActionAttach,
			Channel: "foo",
		})
	}

	if msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); msg.Action != protocol.ActionAttached {
		t.Fatalf("expected ATTACHED, got %v", msg.Action)
	}

	h.publish(t, "foo", &protocol.Message{ID: "m1"})

	msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if msg.Action != protocol.ActionMessage {
		t.Fatalf("Action = %v, want MESSAGE", msg.Action)
	}

	// No further frames should arrive within a short window.
	if err := ws.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, _, err := ws.ReadMessage(); err == nil {
		t.Fatal("received an unexpected extra frame; idempotent attach produced duplicates")
	}
}

func TestAttachSupportsMultipleChannels(t *testing.T) {
	srv, h := newTestServer(t, time.Hour)
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	for _, name := range []string{"foo", "bar"} {
		sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
			Action:  protocol.ActionAttach,
			Channel: name,
		})
	}

	// Two ATTACHEDs arrive (order is not guaranteed across attachments).
	got := make(map[string]bool)
	for range 2 {
		msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
		if msg.Action != protocol.ActionAttached {
			t.Fatalf("Action = %v, want ATTACHED", msg.Action)
		}
		got[msg.Channel] = true
	}
	if !got["foo"] || !got["bar"] {
		t.Fatalf("ATTACHED channels = %v, want both foo and bar", got)
	}

	h.publish(t, "foo", &protocol.Message{ID: "f1"})
	h.publish(t, "bar", &protocol.Message{ID: "b1"})

	seen := map[string]string{}
	for range 2 {
		msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
		if msg.Action != protocol.ActionMessage {
			t.Fatalf("Action = %v, want MESSAGE", msg.Action)
		}
		if len(msg.Messages) != 1 {
			t.Fatalf("Messages length = %d, want 1", len(msg.Messages))
		}
		seen[msg.Channel] = msg.Messages[0].ID
	}
	if seen["foo"] != "f1" || seen["bar"] != "b1" {
		t.Errorf("seen = %v, want foo:f1, bar:b1", seen)
	}
}

func TestPublishAcksAndForwardsToAttachedConnection(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "foo",
	})
	if msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); msg.Action != protocol.ActionAttached {
		t.Fatalf("expected ATTACHED, got %v", msg.Action)
	}

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "foo",
		MsgSerial: 7,
		Messages:  []*protocol.Message{{ID: "m1"}},
	})

	// ACK and the echoed MESSAGE race to the outbound chan; either order
	// is correct.
	frames := map[protocol.Action]*protocol.ProtocolMessage{}
	for range 2 {
		f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
		frames[f.Action] = f
	}

	ack := frames[protocol.ActionAck]
	if ack == nil {
		t.Fatal("no ACK received")
	}
	if ack.MsgSerial != 7 {
		t.Errorf("ACK.MsgSerial = %d, want 7", ack.MsgSerial)
	}
	if ack.Count != 1 {
		t.Errorf("ACK.Count = %d, want 1", ack.Count)
	}

	fwd := frames[protocol.ActionMessage]
	if fwd == nil {
		t.Fatal("no forwarded MESSAGE received")
	}
	if fwd.Channel != "foo" {
		t.Errorf("forwarded Channel = %q, want %q", fwd.Channel, "foo")
	}
	if len(fwd.Messages) != 1 || fwd.Messages[0].ID != "m1" {
		t.Errorf("forwarded payload = %+v, want one msg with ID m1", fwd.Messages)
	}
}

func TestPublishAckCountReflectsBatchSize(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	// No attach: we only care about the ACK here.
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "foo",
		MsgSerial: 3,
		Messages:  []*protocol.Message{{ID: "a"}, {ID: "b"}, {ID: "c"}},
	})

	ack := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if ack.Action != protocol.ActionAck {
		t.Fatalf("Action = %v, want ACK", ack.Action)
	}
	if ack.MsgSerial != 3 {
		t.Errorf("MsgSerial = %d, want 3", ack.MsgSerial)
	}
	if ack.Count != 3 {
		t.Errorf("Count = %d, want 3", ack.Count)
	}
}

func TestPublishWithEmptyChannelIsNacked(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		MsgSerial: 11,
		Messages:  []*protocol.Message{{ID: "x"}},
	})

	msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if msg.Action != protocol.ActionNack {
		t.Fatalf("Action = %v, want NACK", msg.Action)
	}
	if msg.MsgSerial != 11 {
		t.Errorf("MsgSerial = %d, want 11", msg.MsgSerial)
	}
}

func TestPublishWithNoMessagesIsNacked(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "foo",
		MsgSerial: 22,
	})

	msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if msg.Action != protocol.ActionNack {
		t.Fatalf("Action = %v, want NACK", msg.Action)
	}
	if msg.MsgSerial != 22 {
		t.Errorf("MsgSerial = %d, want 22", msg.MsgSerial)
	}
}

func TestPublishCrossesConnections(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	// Subscriber attaches first.
	sub := dial(t, srv, "")
	drainConnected(t, sub)
	sendFrame(t, sub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "foo",
	})
	if msg := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); msg.Action != protocol.ActionAttached {
		t.Fatalf("expected ATTACHED on subscriber, got %v", msg.Action)
	}

	// Publisher (separate connection, no attach).
	pub := dial(t, srv, "")
	drainConnected(t, pub)
	original := &protocol.Message{
		ID:       "hello",
		ClientID: "alice",
		Name:     "greeting",
		Data:     "world",
		Encoding: "utf-8",
	}
	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "foo",
		MsgSerial: 1,
		Messages:  []*protocol.Message{original},
	})

	if ack := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); ack.Action != protocol.ActionAck {
		t.Fatalf("publisher first frame = %v, want ACK", ack.Action)
	}

	fwd := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if fwd.Action != protocol.ActionMessage {
		t.Fatalf("subscriber Action = %v, want MESSAGE", fwd.Action)
	}
	if len(fwd.Messages) != 1 {
		t.Fatalf("subscriber Messages length = %d, want 1", len(fwd.Messages))
	}
	got := fwd.Messages[0]
	if got.ID != original.ID || got.ClientID != original.ClientID || got.Name != original.Name ||
		got.Data != original.Data || got.Encoding != original.Encoding {
		t.Errorf("subscriber payload = %+v, want %+v", got, original)
	}
}

func TestCloseReceivesClosed(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action: protocol.ActionClose,
	})

	msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if msg.Action != protocol.ActionClosed {
		t.Fatalf("Action = %v, want CLOSED", msg.Action)
	}
}

func TestDetachReceivesDetached(t *testing.T) {
	srv, h := newTestServer(t, time.Hour)
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "foo",
	})
	if msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); msg.Action != protocol.ActionAttached {
		t.Fatalf("expected ATTACHED, got %v", msg.Action)
	}

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionDetach,
		Channel: "foo",
	})
	msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if msg.Action != protocol.ActionDetached {
		t.Fatalf("Action = %v, want DETACHED", msg.Action)
	}
	if msg.Channel != "foo" {
		t.Errorf("Channel = %q, want %q", msg.Channel, "foo")
	}

	// Publishing after detach should not forward to this connection.
	h.publish(t, "foo", &protocol.Message{ID: "m1"})
	if err := ws.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, _, err := ws.ReadMessage(); err == nil {
		t.Fatal("received a frame after DETACHED; expected silence on this channel")
	}
}

func TestDetachWithoutAttachIsIdempotent(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionDetach,
		Channel: "never-attached",
	})
	msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if msg.Action != protocol.ActionDetached {
		t.Fatalf("Action = %v, want DETACHED", msg.Action)
	}
}
