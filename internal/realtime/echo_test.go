package realtime

import (
	"context"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/protocol"
)

// dialEcho dials like dialClient but also sets the `echo` query param.
func dialEcho(t *testing.T, srv *httptest.Server, clientID, echo string) *websocket.Conn {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	u.Scheme = "ws"
	q := u.Query()
	q.Set("key", testKey)
	if clientID != "" {
		q.Set("clientId", clientID)
	}
	q.Set("echo", echo)
	u.RawQuery = q.Encode()
	ws, _, err := websocket.DefaultDialer.DialContext(context.Background(), u.String(), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = ws.Close() })
	return ws
}

// expectNoFrame asserts no frame arrives within the window. The server is
// created with a long heartbeat so a HEARTBEAT cannot race the read.
func expectNoFrame(t *testing.T, ws *websocket.Conn, within time.Duration) {
	t.Helper()
	if err := ws.SetReadDeadline(time.Now().Add(within)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, data, err := ws.ReadMessage(); err == nil {
		t.Fatalf("expected no frame, got %q", string(data))
	}
}

// TestEchoFalseSuppressesOwnMessage: a connection opened with echo=false
// is ACKed for its publish but does not receive its own message back,
// while a co-attached peer still receives it (AC #1, #3).
func TestEchoFalseSuppressesOwnMessage(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	// Peer subscriber (default echo) so we can confirm the message is
	// still fanned out to everyone else.
	peer := dial(t, srv, "")
	drainConnected(t, peer)
	attach(t, peer, "room", protocol.FlagSubscribe)

	pub := dialEcho(t, srv, "alice", "false")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPublish|protocol.FlagSubscribe)

	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "room",
		MsgSerial: msgSerialPtr(1),
		Messages:  []*protocol.Message{{Data: "hello"}},
	})

	// The publisher is ACKed...
	if ack := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); ack.Action != protocol.ActionAck {
		t.Fatalf("publisher frame = %v, want ACK", ack.Action)
	}
	// ...but never receives its own message back.
	expectNoFrame(t, pub, 300*time.Millisecond)

	// The peer still receives it (AC #3).
	got := readMessage(t, peer)
	if got.Messages[0].Data != "hello" {
		t.Errorf("peer message = %+v, want data hello", got.Messages[0])
	}
}

// TestEchoTrueReceivesOwnMessage: the default (echo=true) connection does
// receive its own published message back (AC #2).
func TestEchoTrueReceivesOwnMessage(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	pub := dialEcho(t, srv, "alice", "true")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPublish|protocol.FlagSubscribe)

	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "room",
		MsgSerial: msgSerialPtr(1),
		Messages:  []*protocol.Message{{Data: "hi"}},
	})

	// Drain ACK + own echoed MESSAGE (either order).
	var echoed *protocol.ProtocolMessage
	for range 2 {
		f := readFrame(t, pub, protocol.FormatJSON, 2*time.Second)
		if f.Action == protocol.ActionMessage {
			echoed = f
		}
	}
	if echoed == nil {
		t.Fatal("echo=true publisher did not receive its own message")
	}
	if echoed.Messages[0].Data != "hi" {
		t.Errorf("echoed message = %+v, want data hi", echoed.Messages[0])
	}
	if echoed.Messages[0].ConnectionID == "" {
		t.Error("delivered message missing connectionId stamp")
	}
}

// TestEchoFalseStillDeliversOwnPresence: echo=false suppresses a
// connection's own MESSAGEs but never its presence — the connection still
// receives its own ENTER (Ably semantics; DESIGN.md §2.1).
func TestEchoFalseStillDeliversOwnPresence(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	ws := dialEcho(t, srv, "alice", "false")
	drainConnected(t, ws)
	attach(t, ws, "room", protocol.FlagPresence|protocol.FlagPresenceSubscribe)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   "room",
		MsgSerial: msgSerialPtr(1),
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, Data: "here"}},
	})

	var ownPresence *protocol.ProtocolMessage
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && ownPresence == nil {
		f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
		if f.Action == protocol.ActionPresence {
			ownPresence = f
		}
	}
	if ownPresence == nil {
		t.Fatal("echo=false suppressed the connection's own presence; presence must always be delivered")
	}
	if ownPresence.Presence[0].ClientID != "alice" {
		t.Errorf("own presence clientId = %q, want alice", ownPresence.Presence[0].ClientID)
	}
}
