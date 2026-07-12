package realtime

import (
	"context"
	"fmt"
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
	ctx := context.Background()
	ch, err := h.manager.GetChannel(ctx, channel)
	if err != nil {
		t.Fatalf("GetChannel %q: %v", channel, err)
	}
	if _, _, err := ch.Publish(ctx, msgs); err != nil {
		t.Fatalf("publish to %q: %v", channel, err)
	}
}

// msgSerialPtr wraps an inbound frame's msgSerial. ProtocolMessage.MsgSerial
// is a *int64 (TASK-97) so an ACK/NACK carries msgSerial:0 explicitly;
// tests build inbound frames with it and read acks via PublishSerial().
func msgSerialPtr(v int64) *int64 { return &v }

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
	rt := NewServer([]auth.APIKey{parsed}, manager, hb, slog.New(slog.DiscardHandler), nil, nil)
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
	// Even a fresh attach to an empty channel carries a non-empty
	// channelSerial — the storage watermark, so the client always has
	// a resumable cursor (TASK-16).
	if msg.ChannelSerial == "" {
		t.Errorf("ChannelSerial = %q, want non-empty watermark on fresh attach", msg.ChannelSerial)
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

func TestRepeatAttachReattachesWithoutDuplicating(t *testing.T) {
	srv, h := newTestServer(t, time.Hour)
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	// A repeat ATTACH for the same channel is a re-attach (RTL4, DESIGN.md
	// §4.1): each ATTACH gets its own ATTACHED, so the SDK never blocks. But
	// the earlier attachment is torn down, so only one live attachment
	// remains and a subsequent publish produces exactly one MESSAGE.
	for range 2 {
		sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
			Action:  protocol.ActionAttach,
			Channel: "foo",
		})
		if msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); msg.Action != protocol.ActionAttached {
			t.Fatalf("expected ATTACHED, got %v", msg.Action)
		}
	}

	h.publish(t, "foo", &protocol.Message{ID: "m1"})

	msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if msg.Action != protocol.ActionMessage {
		t.Fatalf("Action = %v, want MESSAGE", msg.Action)
	}

	// No further frames should arrive within a short window — the re-attach
	// must not leave a second live attachment delivering duplicates.
	if err := ws.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, _, err := ws.ReadMessage(); err == nil {
		t.Fatal("received an unexpected extra frame; re-attach produced duplicates")
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
		MsgSerial: msgSerialPtr(7),
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
	if ack.PublishSerial() != 7 {
		t.Errorf("ACK.MsgSerial = %d, want 7", ack.PublishSerial())
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

// TestPublishAckIsPerProtocolMessage: an ACK acknowledges one protocol
// message (Count=1) regardless of how many messages the frame carries —
// msgSerial is per frame, not per message. The per-message serials ride
// the ACK's Res entry instead.
func TestPublishAckIsPerProtocolMessage(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	// No attach: we only care about the ACK here.
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "foo",
		MsgSerial: msgSerialPtr(3),
		Messages:  []*protocol.Message{{ID: "batch:0"}, {ID: "batch:1"}, {ID: "batch:2"}},
	})

	ack := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if ack.Action != protocol.ActionAck {
		t.Fatalf("Action = %v, want ACK", ack.Action)
	}
	if ack.PublishSerial() != 3 {
		t.Errorf("MsgSerial = %d, want 3", ack.PublishSerial())
	}
	if ack.Count != 1 {
		t.Errorf("Count = %d, want 1 (one protocol message acked, not the batch size)", ack.Count)
	}
	// The three assigned serials ride a single Res entry for the frame.
	if len(ack.Res) != 1 {
		t.Fatalf("Res length = %d, want 1 (one entry per acked frame)", len(ack.Res))
	}
	if len(ack.Res[0].Serials) != 3 {
		t.Errorf("Res[0].Serials = %v, want 3 serials for the 3-message batch", ack.Res[0].Serials)
	}
}

func TestPublishWithEmptyChannelIsNacked(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		MsgSerial: msgSerialPtr(11),
		Messages:  []*protocol.Message{{ID: "x"}},
	})

	msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if msg.Action != protocol.ActionNack {
		t.Fatalf("Action = %v, want NACK", msg.Action)
	}
	if msg.PublishSerial() != 11 {
		t.Errorf("MsgSerial = %d, want 11", msg.PublishSerial())
	}
}

func TestPublishWithNoMessagesIsNacked(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "foo",
		MsgSerial: msgSerialPtr(22),
	})

	msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if msg.Action != protocol.ActionNack {
		t.Fatalf("Action = %v, want NACK", msg.Action)
	}
	if msg.PublishSerial() != 22 {
		t.Errorf("MsgSerial = %d, want 22", msg.PublishSerial())
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
		MsgSerial: msgSerialPtr(1),
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

// resumeAttachAndDrain ATTACHes on a new WS with the given resume
// cursor, reads frames until ATTACHED, collects subsequent MESSAGE
// frames until the next non-MESSAGE arrives or a short idle, and
// returns the ATTACHED frame plus the replayed cms in order.
func resumeAttachAndDrain(t *testing.T, srv *httptest.Server, channel, resumeFrom string) (attached *protocol.ProtocolMessage, replayed []*protocol.ProtocolMessage) {
	t.Helper()
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:        protocol.ActionAttach,
		Channel:       channel,
		ChannelSerial: resumeFrom,
	})

	attached = readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if attached.Action != protocol.ActionAttached {
		t.Fatalf("first frame = %v, want ATTACHED", attached.Action)
	}

	// Drain replayed MESSAGE frames; stop on idle.
	for {
		if err := ws.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			t.Fatalf("SetReadDeadline: %v", err)
		}
		_, data, err := ws.ReadMessage()
		if err != nil {
			break
		}
		var f protocol.ProtocolMessage
		if err := protocol.Unmarshal(data, protocol.FormatJSON, &f); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if f.Action != protocol.ActionMessage {
			t.Fatalf("unexpected frame during replay drain: %v", f.Action)
		}
		replayed = append(replayed, &f)
	}
	_ = ws.Close()
	return attached, replayed
}

func TestResumeReplaysGapAndSetsResumedFlag(t *testing.T) {
	srv, h := newTestServer(t, time.Hour)

	// Publish 5 messages first; capture the channelSerial of each.
	for i := range 5 {
		h.publish(t, "foo", &protocol.Message{ID: fmt.Sprintf("m%d", i)})
	}
	// Read each delivered channelSerial via a fresh attach.
	ws := dial(t, srv, "")
	drainConnected(t, ws)
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "foo"})
	if first := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); first.Action != protocol.ActionAttached {
		t.Fatalf("ATTACHED expected, got %v", first.Action)
	}
	// No publishes happen after this attach — there are no MESSAGE frames to drain.
	// To capture per-publish serials, publish + read in lockstep on a fresh attach below.
	_ = ws.Close()

	ws2 := dial(t, srv, "")
	drainConnected(t, ws2)
	sendFrame(t, ws2, protocol.FormatJSON, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "bar"})
	if first := readFrame(t, ws2, protocol.FormatJSON, 2*time.Second); first.Action != protocol.ActionAttached {
		t.Fatalf("ATTACHED expected, got %v", first.Action)
	}
	var serials []string
	for i := range 5 {
		h.publish(t, "bar", &protocol.Message{ID: fmt.Sprintf("b%d", i)})
		f := readFrame(t, ws2, protocol.FormatJSON, 2*time.Second)
		serials = append(serials, f.ChannelSerial)
	}
	_ = ws2.Close()

	// Resume on "bar" with the serial of b1 — expect ATTACHED w/ RESUMED set
	// and replay of b2, b3, b4.
	attached, replayed := resumeAttachAndDrain(t, srv, "bar", serials[1])
	if attached.Flags&protocol.FlagResumed == 0 {
		t.Errorf("Flags = %v, want RESUMED (= %v) set", attached.Flags, protocol.FlagResumed)
	}
	if attached.Error != nil {
		t.Errorf("Error = %+v, want nil on full replay", attached.Error)
	}
	if attached.ChannelSerial != serials[1] {
		t.Errorf("ATTACHED.ChannelSerial = %q, want %q (echoes client cursor)", attached.ChannelSerial, serials[1])
	}
	if len(replayed) != 3 {
		t.Fatalf("replayed len = %d, want 3 (b2, b3, b4); got %+v", len(replayed), replayed)
	}
	for i, want := range []string{"b2", "b3", "b4"} {
		if got := replayed[i].Messages[0].ID; got != want {
			t.Errorf("replayed[%d].Messages[0].ID = %q, want %q", i, got, want)
		}
	}
}

func TestResumeWhenCaughtUpHasNoReplay(t *testing.T) {
	srv, h := newTestServer(t, time.Hour)

	ws := dial(t, srv, "")
	drainConnected(t, ws)
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "foo"})
	if first := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); first.Action != protocol.ActionAttached {
		t.Fatalf("ATTACHED expected, got %v", first.Action)
	}
	h.publish(t, "foo", &protocol.Message{ID: "m1"})
	msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	cursorAtHead := msg.ChannelSerial
	_ = ws.Close()

	// Resume with the most recent serial — client is caught up.
	attached, replayed := resumeAttachAndDrain(t, srv, "foo", cursorAtHead)
	if attached.Flags&protocol.FlagResumed == 0 {
		t.Errorf("Flags = %v, want RESUMED set (caught-up resume is a successful resume)", attached.Flags)
	}
	if len(replayed) != 0 {
		t.Errorf("replayed len = %d, want 0 (caught up); got %+v", len(replayed), replayed)
	}
}

func TestResumeAgainstEmptyChannelIsCaughtUp(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	// "empty-chan" has no publishes. The client supplies a cursor and
	// asks to resume. Without retention, "exhausted the backwards walk
	// with the client's cursor not matched" is treated as "you're
	// caught up, here's nothing" — RESUMED set, no error, no replay.
	// (TASK-26 will refine this once retention can drop cms below an
	// in-storage cursor.)
	attached, replayed := resumeAttachAndDrain(t, srv, "empty-chan", "00000000000001-000@aaaaaaaaaa:000")
	if attached.Flags&protocol.FlagResumed == 0 {
		t.Errorf("Flags = %v, want RESUMED set (exhausted storage, nothing to deliver)", attached.Flags)
	}
	if len(replayed) != 0 {
		t.Errorf("replayed len = %d, want 0", len(replayed))
	}
	if attached.Error != nil {
		t.Errorf("Error = %+v, want nil", attached.Error)
	}
}

func TestResumeCapExceededDeliversNewestCapWithError(t *testing.T) {
	srv, h := newTestServer(t, time.Hour)

	// Use a low replay cap by reaching into the package internals: we
	// can't change defaultReplayCap without rebuilding, so instead we
	// publish more than the cap (~1000). For test speed, publish 1100.
	const total = defaultReplayCap + 100
	const ancientCursor = "00000000000001-000@aaaaaaaaaa:000"

	for i := range total {
		h.publish(t, "cap-chan", &protocol.Message{ID: fmt.Sprintf("m%d", i)})
	}

	attached, replayed := resumeAttachAndDrain(t, srv, "cap-chan", ancientCursor)
	if attached.Flags&protocol.FlagResumed != 0 {
		t.Errorf("Flags = %v, want RESUMED clear (cap exceeded)", attached.Flags)
	}
	if attached.Error == nil {
		t.Error("Error = nil, want an ErrorInfo explaining the truncation")
	}
	if len(replayed) != defaultReplayCap {
		t.Fatalf("replayed len = %d, want %d (newest cap)", len(replayed), defaultReplayCap)
	}
	// The newest cap should be m100..m1099.
	for i := range defaultReplayCap {
		want := fmt.Sprintf("m%d", i+(total-defaultReplayCap))
		if got := replayed[i].Messages[0].ID; got != want {
			t.Fatalf("replayed[%d].Messages[0].ID = %q, want %q", i, got, want)
		}
	}
}

// rewindAttachAndDrain ATTACHes on a new WS with params={"rewind": v},
// reads frames until ATTACHED, collects subsequent MESSAGE frames
// until idle, and returns the ATTACHED frame plus the replayed cms in
// order.
func rewindAttachAndDrain(t *testing.T, srv *httptest.Server, channel, rewind string) (attached *protocol.ProtocolMessage, replayed []*protocol.ProtocolMessage) {
	t.Helper()
	ws := dial(t, srv, "")
	drainConnected(t, ws)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: channel,
		Params:  map[string]string{"rewind": rewind},
	})

	attached = readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if attached.Action != protocol.ActionAttached {
		t.Fatalf("first frame = %v, want ATTACHED", attached.Action)
	}
	for {
		if err := ws.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			t.Fatalf("SetReadDeadline: %v", err)
		}
		_, data, err := ws.ReadMessage()
		if err != nil {
			break
		}
		var f protocol.ProtocolMessage
		if err := protocol.Unmarshal(data, protocol.FormatJSON, &f); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if f.Action != protocol.ActionMessage {
			t.Fatalf("unexpected frame: %v", f.Action)
		}
		replayed = append(replayed, &f)
	}
	_ = ws.Close()
	return attached, replayed
}

func TestRewindCountReplaysNewestN(t *testing.T) {
	srv, h := newTestServer(t, time.Hour)
	for i := range 5 {
		h.publish(t, "rwc", &protocol.Message{ID: fmt.Sprintf("m%d", i)})
	}

	attached, replayed := rewindAttachAndDrain(t, srv, "rwc", "3")
	if attached.Flags&protocol.FlagResumed != 0 {
		t.Errorf("Flags = %v, want RESUMED clear (rewind is not a resume)", attached.Flags)
	}
	if attached.Error != nil {
		t.Errorf("Error = %+v, want nil for a clean rewind", attached.Error)
	}
	if got := attached.Params["rewind"]; got != "3" {
		t.Errorf("ATTACHED.Params[rewind] = %q, want %q", got, "3")
	}
	if len(replayed) != 3 {
		t.Fatalf("replayed len = %d, want 3", len(replayed))
	}
	for i, want := range []string{"m2", "m3", "m4"} {
		if got := replayed[i].Messages[0].ID; got != want {
			t.Errorf("replayed[%d] = %q, want %q", i, got, want)
		}
	}
	// ATTACHED.channelSerial must sort strictly before the first
	// replayed message — the "predecessor" serial.
	if !(attached.ChannelSerial < replayed[0].ChannelSerial) {
		t.Errorf("attached.ChannelSerial %q not < first replayed %q", attached.ChannelSerial, replayed[0].ChannelSerial)
	}
}

func TestRewindCountCoveringEntireChannelUsesInitialSerial(t *testing.T) {
	srv, h := newTestServer(t, time.Hour)
	for i := range 2 {
		h.publish(t, "rwa", &protocol.Message{ID: fmt.Sprintf("m%d", i)})
	}

	// Rewind asks for more than exists: deliver all, use channel's
	// initial serial as the attach point.
	attached, replayed := rewindAttachAndDrain(t, srv, "rwa", "10")
	if len(replayed) != 2 {
		t.Fatalf("replayed len = %d, want 2", len(replayed))
	}
	if !(attached.ChannelSerial < replayed[0].ChannelSerial) {
		t.Errorf("attached.ChannelSerial %q not < first replayed %q (should be channel initial serial)",
			attached.ChannelSerial, replayed[0].ChannelSerial)
	}
}

func TestRewindDurationReplaysWindow(t *testing.T) {
	srv, h := newTestServer(t, time.Hour)
	// Publish 3 messages, sleep, publish 3 more — the rewind window
	// should capture only the recent batch.
	for i := range 3 {
		h.publish(t, "rwd", &protocol.Message{ID: fmt.Sprintf("old%d", i)})
	}
	time.Sleep(750 * time.Millisecond)
	for i := range 3 {
		h.publish(t, "rwd", &protocol.Message{ID: fmt.Sprintf("new%d", i)})
	}

	attached, replayed := rewindAttachAndDrain(t, srv, "rwd", "0.5s")
	if attached.Flags&protocol.FlagResumed != 0 {
		t.Errorf("Flags = %v, want RESUMED clear", attached.Flags)
	}
	if len(replayed) != 3 {
		t.Fatalf("replayed len = %d, want 3 (only the 'new' batch); got %d", len(replayed), len(replayed))
	}
	for i, want := range []string{"new0", "new1", "new2"} {
		if got := replayed[i].Messages[0].ID; got != want {
			t.Errorf("replayed[%d] = %q, want %q", i, got, want)
		}
	}
}

func TestRewindInvalidParamYieldsErrorNoReplay(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	attached, replayed := rewindAttachAndDrain(t, srv, "rwbad", "abc")
	if attached.Error == nil {
		t.Error("Error = nil, want a 400-class ErrorInfo for invalid rewind")
	}
	if len(replayed) != 0 {
		t.Errorf("replayed len = %d, want 0 on invalid rewind", len(replayed))
	}
}

func TestRewindIsIgnoredWhenChannelSerialIsSupplied(t *testing.T) {
	srv, h := newTestServer(t, time.Hour)
	// Publish 5 to establish history with known serials.
	ws := dial(t, srv, "")
	drainConnected(t, ws)
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "rwconflict"})
	if first := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); first.Action != protocol.ActionAttached {
		t.Fatalf("ATTACHED expected, got %v", first.Action)
	}
	var serials []string
	for i := range 5 {
		h.publish(t, "rwconflict", &protocol.Message{ID: fmt.Sprintf("m%d", i)})
		f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
		serials = append(serials, f.ChannelSerial)
	}
	_ = ws.Close()

	// Resume with channelSerial=serials[2] AND rewind=999. Resume wins:
	// replay must be the gap m3..m4 (2 messages), not the rewind window.
	ws2 := dial(t, srv, "")
	drainConnected(t, ws2)
	sendFrame(t, ws2, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:        protocol.ActionAttach,
		Channel:       "rwconflict",
		ChannelSerial: serials[2],
		Params:        map[string]string{"rewind": "999"},
	})
	attached := readFrame(t, ws2, protocol.FormatJSON, 2*time.Second)
	if attached.Action != protocol.ActionAttached {
		t.Fatalf("first = %v, want ATTACHED", attached.Action)
	}
	if attached.Flags&protocol.FlagResumed == 0 {
		t.Errorf("Flags = %v, want RESUMED set (channelSerial wins → resume path)", attached.Flags)
	}
	var got []string
	for {
		if err := ws2.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			t.Fatalf("SetReadDeadline: %v", err)
		}
		_, data, err := ws2.ReadMessage()
		if err != nil {
			break
		}
		var f protocol.ProtocolMessage
		if err := protocol.Unmarshal(data, protocol.FormatJSON, &f); err != nil {
			t.Fatalf("decode: %v", err)
		}
		got = append(got, f.Messages[0].ID)
	}
	_ = ws2.Close()
	if !equalStringSlices(got, []string{"m3", "m4"}) {
		t.Errorf("got %v, want [m3 m4] (rewind=999 ignored, channelSerial gap replayed)", got)
	}
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// dialClientID connects like dial but sets the clientId query param, so
// the connection resolves to a concrete identity (DESIGN.md §3.2).
func dialClientID(t *testing.T, srv *httptest.Server, clientID string) *websocket.Conn {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	u.Scheme = "ws"
	q := u.Query()
	q.Set("key", testKey)
	q.Set("clientId", clientID)
	u.RawQuery = q.Encode()
	ws, _, err := websocket.DefaultDialer.DialContext(context.Background(), u.String(), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = ws.Close() })
	return ws
}

// TestPublishStampsAndRejectsClientID covers the §3.2 message rules on a
// concrete-clientId connection: an omitted clientId is stamped with the
// connection's, and a mismatched clientId is NACKed.
func TestPublishStampsAndRejectsClientID(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	sub := dial(t, srv, "")
	drainConnected(t, sub)
	sendFrame(t, sub, protocol.FormatJSON, &protocol.ProtocolMessage{Action: protocol.ActionAttach, Channel: "room"})
	if msg := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); msg.Action != protocol.ActionAttached {
		t.Fatalf("expected ATTACHED, got %v", msg.Action)
	}

	pub := dialClientID(t, srv, "alice")
	drainConnected(t, pub)

	// Omitted clientId is stamped with the connection's resolved identity.
	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "room",
		MsgSerial: msgSerialPtr(1),
		Messages:  []*protocol.Message{{Name: "n", Data: "d"}},
	})
	if ack := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); ack.Action != protocol.ActionAck {
		t.Fatalf("publisher frame = %v, want ACK", ack.Action)
	}
	delivered := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if delivered.Action != protocol.ActionMessage || len(delivered.Messages) != 1 {
		t.Fatalf("subscriber frame = %v (%d msgs)", delivered.Action, len(delivered.Messages))
	}
	if got := delivered.Messages[0].ClientID; got != "alice" {
		t.Errorf("stamped clientId = %q, want alice", got)
	}

	// A message asserting a different clientId is rejected.
	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "room",
		MsgSerial: msgSerialPtr(2),
		Messages:  []*protocol.Message{{Name: "n", Data: "d", ClientID: "bob"}},
	})
	if nack := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); nack.Action != protocol.ActionNack {
		t.Fatalf("publisher frame = %v, want NACK", nack.Action)
	}
}
