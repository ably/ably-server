package realtime

import (
	"context"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/protocol"
)

func TestResolvePresenceClientID(t *testing.T) {
	cases := []struct {
		name      string
		conn, msg string
		wantID    string
		wantOK    bool
	}{
		{"anonymous rejected", "", "", "", false},
		{"anonymous rejected even with msg id", "", "alice", "", false},
		{"concrete omitted stamps conn", "alice", "", "alice", true},
		{"concrete matching", "alice", "alice", "alice", true},
		{"concrete mismatch rejected", "alice", "bob", "", false},
		{"wildcard concrete ok", "*", "bob", "bob", true},
		{"wildcard must supply id", "*", "", "", false},
		{"wildcard star itself rejected", "*", "*", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotID, gotOK := resolvePresenceClientID(tc.conn, tc.msg)
			if gotID != tc.wantID || gotOK != tc.wantOK {
				t.Errorf("resolvePresenceClientID(%q, %q) = (%q, %v), want (%q, %v)",
					tc.conn, tc.msg, gotID, gotOK, tc.wantID, tc.wantOK)
			}
		})
	}
}

// dialClient dials like dial but also sets the clientId query param.
func dialClient(t *testing.T, srv *httptest.Server, clientID string) *websocket.Conn {
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
	u.RawQuery = q.Encode()
	ws, _, err := websocket.DefaultDialer.DialContext(context.Background(), u.String(), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = ws.Close() })
	return ws
}

// attach sends ATTACH with the given mode flags and drains the ATTACHED.
func attach(t *testing.T, ws *websocket.Conn, channel string, flags int64) {
	t.Helper()
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: channel,
		Flags:   flags,
	})
	if msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); msg.Action != protocol.ActionAttached {
		t.Fatalf("expected ATTACHED on %q, got %v", channel, msg.Action)
	}
}

// enter sends a single ENTER on channel and reads the ACK. The caller's
// connection must be attached with the PRESENCE mode (and, to avoid a
// self-echo confusing the ACK read, without PRESENCE_SUBSCRIBE).
func enter(t *testing.T, ws *websocket.Conn, channel string, msgSerial int64) {
	t.Helper()
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   channel,
		MsgSerial: protocol.Int64(msgSerial),
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter}},
	})
	if ack := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); ack.Action != protocol.ActionAck {
		t.Fatalf("enter: frame = %v, want ACK", ack.Action)
	}
}

// sendAttach sends ATTACH with flags and returns the ATTACHED frame so
// the caller can inspect its flags (unlike attach, which discards it).
func sendAttach(t *testing.T, ws *websocket.Conn, channel string, flags int64) *protocol.ProtocolMessage {
	t.Helper()
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: channel,
		Flags:   flags,
	})
	msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if msg.Action != protocol.ActionAttached {
		t.Fatalf("expected ATTACHED on %q, got %v", channel, msg.Action)
	}
	return msg
}

// TestPresenceSyncOnAttach: attaching to a channel with members sets
// HAS_PRESENCE on ATTACHED and delivers the set as a SYNC frame (action
// PRESENT), with the cursor marking the set complete.
func TestPresenceSyncOnAttach(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPresence)
	enter(t, pub, "room", 1)

	sub := dial(t, srv, "")
	drainConnected(t, sub)
	attached := sendAttach(t, sub, "room", 0) // full modes incl. PRESENCE_SUBSCRIBE
	if attached.Flags&protocol.FlagHasPresence == 0 {
		t.Errorf("ATTACHED flags = %d, want HAS_PRESENCE set", attached.Flags)
	}

	sync := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if sync.Action != protocol.ActionSync {
		t.Fatalf("Action = %v, want SYNC", sync.Action)
	}
	if len(sync.Presence) != 1 {
		t.Fatalf("SYNC Presence length = %d, want 1", len(sync.Presence))
	}
	p := sync.Presence[0]
	if p.ClientID != "alice" {
		t.Errorf("member clientId = %q, want alice", p.ClientID)
	}
	if p.Action != protocol.PresencePresent {
		t.Errorf("member action = %v, want present", p.Action)
	}
	if !strings.HasSuffix(sync.ChannelSerial, ":") {
		t.Errorf("SYNC channelSerial = %q, want a trailing ':' marking the set complete", sync.ChannelSerial)
	}
}

// TestPresenceNoSyncWhenEmpty: an empty channel yields no HAS_PRESENCE
// and no SYNC frame.
func TestPresenceNoSyncWhenEmpty(t *testing.T) {
	srv, h := newTestServer(t, time.Hour)
	sub := dial(t, srv, "")
	drainConnected(t, sub)
	attached := sendAttach(t, sub, "room", 0)
	if attached.Flags&protocol.FlagHasPresence != 0 {
		t.Errorf("ATTACHED flags = %d, want HAS_PRESENCE clear for empty channel", attached.Flags)
	}
	// A subsequent message is the next frame — proving no SYNC was sent.
	h.publish(t, "room", &protocol.Message{Name: "m"})
	next := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if next.Action != protocol.ActionMessage {
		t.Fatalf("next frame = %v, want MESSAGE (a stray SYNC would arrive first)", next.Action)
	}
}

// TestPresenceNoSyncWithoutSubscribeMode: an attachment lacking
// PRESENCE_SUBSCRIBE gets neither HAS_PRESENCE nor a SYNC even when the
// channel has members.
func TestPresenceNoSyncWithoutSubscribeMode(t *testing.T) {
	srv, h := newTestServer(t, time.Hour)

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPresence)
	enter(t, pub, "room", 1)

	sub := dial(t, srv, "")
	drainConnected(t, sub)
	attached := sendAttach(t, sub, "room", protocol.FlagSubscribe) // no PRESENCE_SUBSCRIBE
	if attached.Flags&protocol.FlagHasPresence != 0 {
		t.Errorf("ATTACHED flags = %d, want HAS_PRESENCE clear (no PRESENCE_SUBSCRIBE)", attached.Flags)
	}
	h.publish(t, "room", &protocol.Message{Name: "m"})
	next := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if next.Action != protocol.ActionMessage {
		t.Fatalf("next frame = %v, want MESSAGE (no SYNC expected)", next.Action)
	}
}

// TestPresenceSyncThenLiveConverges: the pre-existing member arrives via
// SYNC, a member that joins afterwards arrives live — the subscriber sees
// both, with no duplicate of the synced member.
func TestPresenceSyncThenLiveConverges(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	a := dialClient(t, srv, "alice")
	drainConnected(t, a)
	attach(t, a, "room", protocol.FlagPresence)
	enter(t, a, "room", 1)

	sub := dial(t, srv, "")
	drainConnected(t, sub)
	attached := sendAttach(t, sub, "room", 0)
	if attached.Flags&protocol.FlagHasPresence == 0 {
		t.Fatalf("want HAS_PRESENCE set")
	}
	sync := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if sync.Action != protocol.ActionSync || len(sync.Presence) != 1 || sync.Presence[0].ClientID != "alice" {
		t.Fatalf("SYNC = %v with %d members, want alice", sync.Action, len(sync.Presence))
	}

	// bob joins after the sync → delivered live (and alice is NOT
	// re-delivered live, since her enter precedes the subscriber's anchor).
	b := dialClient(t, srv, "bob")
	drainConnected(t, b)
	attach(t, b, "room", protocol.FlagPresence)
	enter(t, b, "room", 1)

	live := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if live.Action != protocol.ActionPresence || len(live.Presence) != 1 {
		t.Fatalf("live frame = %v (presence len %d), want one PRESENCE", live.Action, len(live.Presence))
	}
	if live.Presence[0].ClientID != "bob" || live.Presence[0].Action != protocol.PresenceEnter {
		t.Errorf("live member = (%q, %v), want (bob, enter)", live.Presence[0].ClientID, live.Presence[0].Action)
	}
}

// TestPresenceEnterCrossesConnections: a subscriber receives a member's
// ENTER as a PRESENCE frame carrying the resolved clientId + connectionId.
func TestPresenceEnterCrossesConnections(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	sub := dial(t, srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", 0) // full modes incl. PRESENCE_SUBSCRIBE

	// Publisher enters with its own clientId; attaches PRESENCE-only so
	// it does not receive its own echo.
	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPresence)

	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   "room",
		MsgSerial: protocol.Int64(1),
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, Data: "hi"}},
	})
	if ack := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); ack.Action != protocol.ActionAck {
		t.Fatalf("publisher first frame = %v, want ACK", ack.Action)
	}

	fwd := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if fwd.Action != protocol.ActionPresence {
		t.Fatalf("subscriber Action = %v, want PRESENCE", fwd.Action)
	}
	if len(fwd.Presence) != 1 {
		t.Fatalf("Presence length = %d, want 1", len(fwd.Presence))
	}
	p := fwd.Presence[0]
	if p.Action != protocol.PresenceEnter {
		t.Errorf("action = %v, want enter", p.Action)
	}
	if p.ClientID != "alice" {
		t.Errorf("clientId = %q, want alice (server-resolved)", p.ClientID)
	}
	if p.ConnectionID == "" {
		t.Error("connectionId not stamped")
	}
	if p.Serial == "" {
		t.Error("serial not stamped")
	}
}

// TestPresenceUpdateAndLeaveDelivered: update and leave reach subscribers
// in order, each carrying the right action.
func TestPresenceUpdateAndLeaveDelivered(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	sub := dial(t, srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", 0)

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPresence)

	for i, action := range []protocol.PresenceAction{protocol.PresenceEnter, protocol.PresenceUpdate, protocol.PresenceLeave} {
		sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
			Action:    protocol.ActionPresence,
			Channel:   "room",
			MsgSerial: protocol.Int64(int64(i + 1)),
			Presence:  []*protocol.PresenceMessage{{Action: action}},
		})
		if ack := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); ack.Action != protocol.ActionAck {
			t.Fatalf("op %d: publisher frame = %v, want ACK", i, ack.Action)
		}
		fwd := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
		if fwd.Action != protocol.ActionPresence || len(fwd.Presence) != 1 {
			t.Fatalf("op %d: subscriber frame = %v (presence len %d)", i, fwd.Action, len(fwd.Presence))
		}
		if fwd.Presence[0].Action != action {
			t.Errorf("op %d: delivered action = %v, want %v", i, fwd.Presence[0].Action, action)
		}
	}
}

// TestPresenceAnonymousRejected: a connection with no clientId cannot
// enter presence — NACK with code 91000.
func TestPresenceAnonymousRejected(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dial(t, srv, "") // no clientId
	drainConnected(t, ws)
	attach(t, ws, "room", 0)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   "room",
		MsgSerial: protocol.Int64(7),
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter}},
	})
	msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if msg.Action != protocol.ActionNack {
		t.Fatalf("Action = %v, want NACK", msg.Action)
	}
	if msg.MsgSerialValue() != 7 {
		t.Errorf("MsgSerial = %d, want 7", msg.MsgSerialValue())
	}
	if msg.Error == nil || msg.Error.Code != 91000 {
		t.Errorf("Error = %+v, want code 91000", msg.Error)
	}
}

// TestPresenceClientIDMismatchRejected: a concrete connection cannot
// enter as a different clientId.
func TestPresenceClientIDMismatchRejected(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dialClient(t, srv, "alice")
	drainConnected(t, ws)
	attach(t, ws, "room", 0)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   "room",
		MsgSerial: protocol.Int64(3),
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, ClientID: "bob"}},
	})
	msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if msg.Action != protocol.ActionNack {
		t.Fatalf("Action = %v, want NACK (clientId mismatch)", msg.Action)
	}
}

// TestPresenceRequiresPresenceMode: an attachment without the PRESENCE
// mode cannot enter presence.
func TestPresenceRequiresPresenceMode(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dialClient(t, srv, "alice")
	drainConnected(t, ws)
	attach(t, ws, "room", protocol.FlagSubscribe) // subscribe only, no presence

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   "room",
		MsgSerial: protocol.Int64(5),
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter}},
	})
	msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if msg.Action != protocol.ActionNack {
		t.Fatalf("Action = %v, want NACK (no presence mode)", msg.Action)
	}
}

// TestPresenceImplicitLeaveOnDisconnect: when a member's connection
// drops, subscribers receive a synthesised LEAVE.
func TestPresenceImplicitLeaveOnDisconnect(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	sub := dial(t, srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", 0)

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPresence)
	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   "room",
		MsgSerial: protocol.Int64(1),
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter}},
	})
	if ack := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); ack.Action != protocol.ActionAck {
		t.Fatalf("publisher frame = %v, want ACK", ack.Action)
	}

	// Subscriber sees the ENTER, then the LEAVE once pub drops.
	if fwd := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); fwd.Action != protocol.ActionPresence || fwd.Presence[0].Action != protocol.PresenceEnter {
		t.Fatalf("expected ENTER on subscriber, got %v", fwd.Action)
	}
	_ = pub.Close() // abrupt disconnect

	fwd := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if fwd.Action != protocol.ActionPresence || len(fwd.Presence) != 1 {
		t.Fatalf("expected PRESENCE LEAVE, got %v", fwd.Action)
	}
	if fwd.Presence[0].Action != protocol.PresenceLeave {
		t.Errorf("action = %v, want leave", fwd.Presence[0].Action)
	}
	if fwd.Presence[0].ClientID != "alice" {
		t.Errorf("leave clientId = %q, want alice", fwd.Presence[0].ClientID)
	}
}

// TestPresenceDetachLeaves: detaching from a channel leaves the members
// the connection entered on it.
func TestPresenceDetachLeaves(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	sub := dial(t, srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", 0)

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPresence)
	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   "room",
		MsgSerial: protocol.Int64(1),
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter}},
	})
	if ack := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); ack.Action != protocol.ActionAck {
		t.Fatalf("publisher frame = %v, want ACK", ack.Action)
	}
	if fwd := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); fwd.Action != protocol.ActionPresence {
		t.Fatalf("expected ENTER, got %v", fwd.Action)
	}

	// pub detaches → LEAVE for alice reaches the subscriber.
	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionDetach,
		Channel: "room",
	})
	fwd := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if fwd.Action != protocol.ActionPresence || fwd.Presence[0].Action != protocol.PresenceLeave {
		t.Fatalf("expected PRESENCE LEAVE after detach, got %v", fwd.Action)
	}
}
