package realtime

import (
	"context"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/protocol"
)

// dialToken connects a WebSocket client authenticating with a JWT
// carrying the given capability claim (via the access_token query param).
func dialToken(t *testing.T, srv *httptest.Server, capability string) *websocket.Conn {
	return dialTokenClientID(t, srv, capability, "")
}

// dialTokenClientID is dialToken with an optional x-ably-clientId claim.
func dialTokenClientID(t *testing.T, srv *httptest.Server, capability, clientID string) *websocket.Conn {
	t.Helper()
	now := time.Now()
	claims := jwt.MapClaims{"iat": now.Unix(), "exp": now.Add(time.Hour).Unix()}
	if capability != "" {
		claims["x-ably-capability"] = capability
	}
	if clientID != "" {
		claims["x-ably-clientId"] = clientID
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tok.Header["kid"] = "app.key"
	signed, err := tok.SignedString([]byte("secret"))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	u, _ := url.Parse(srv.URL)
	u.Scheme = "ws"
	q := u.Query()
	q.Set("access_token", signed)
	u.RawQuery = q.Encode()
	ws, _, err := websocket.DefaultDialer.DialContext(context.Background(), u.String(), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = ws.Close() })
	return ws
}

func TestAttachModesFromCapability(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	// subscribe-only on chat:* — an attach requesting the full set is
	// narrowed to SUBSCRIBE|PRESENCE_SUBSCRIBE.
	ws := dialToken(t, srv, `{"chat:*":["subscribe"]}`)
	drainConnected(t, ws)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "chat:room",
	})
	f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if f.Action != protocol.ActionAttached {
		t.Fatalf("action = %v, want ATTACHED", f.Action)
	}
	wantModes := protocol.FlagSubscribe | protocol.FlagPresenceSubscribe
	if f.Flags&modeMask != wantModes {
		t.Errorf("ATTACHED modes = %b, want %b", f.Flags&modeMask, wantModes)
	}

	// Requesting only PUBLISH on chat:* yields an empty intersection →
	// ERROR 40160, no attach.
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "chat:other",
		Flags:   protocol.FlagPublish,
	})
	f = readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if f.Action != protocol.ActionError || f.Error == nil || f.Error.Code != 40160 {
		t.Fatalf("frame = %+v, want ERROR 40160", f)
	}

	// A channel outside the capability's resource: empty intersection.
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: "other",
	})
	f = readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if f.Action != protocol.ActionError || f.Error == nil || f.Error.Code != 40160 {
		t.Fatalf("frame = %+v, want ERROR 40160", f)
	}
}

func TestInboundMessageCapability(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	// subscribe-only: a publish must be NACKed with 40160.
	ws := dialToken(t, srv, `{"chat:*":["subscribe"]}`)
	drainConnected(t, ws)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "chat:room",
		MsgSerial: 1,
		Messages:  []*protocol.Message{{Name: "n", Data: "x"}},
	})
	f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if f.Action != protocol.ActionNack || f.Error == nil || f.Error.Code != 40160 {
		t.Fatalf("frame = %+v, want NACK 40160", f)
	}

	// publish granted: the same publish is ACKed.
	ws2 := dialToken(t, srv, `{"chat:*":["publish"]}`)
	drainConnected(t, ws2)
	sendFrame(t, ws2, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "chat:room",
		MsgSerial: 1,
		Messages:  []*protocol.Message{{Name: "n", Data: "x"}},
	})
	f = readFrame(t, ws2, protocol.FormatJSON, 2*time.Second)
	if f.Action != protocol.ActionAck {
		t.Fatalf("frame = %+v, want ACK", f)
	}
}

// sendUpdate sends an update mutation targeting serial with the given
// publish msgSerial and returns the resulting ACK/NACK frame. msgSerial must
// be monotonic per connection (the server drops a repeated/backward serial,
// see acceptMsgSerial), so a connection that already published must pass a
// higher value than its previous frame.
func sendUpdate(t *testing.T, ws *websocket.Conn, channel, serial string, msgSerial int64) *protocol.ProtocolMessage {
	t.Helper()
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   channel,
		MsgSerial: msgSerial,
		Messages:  []*protocol.Message{{Action: protocol.MessageUpdate, Serial: serial, Data: "edited"}},
	})
	return readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
}

func TestWSMutationOwnership(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	const channel = "chat:room"

	// alice publishes a create; she is the creator.
	alice := dialTokenClientID(t, srv, `{"chat:*":["publish","message-update-own"]}`, "alice")
	drainConnected(t, alice)
	sendFrame(t, alice, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   channel,
		MsgSerial: 1,
		Messages:  []*protocol.Message{{Name: "n", Data: "orig"}},
	})
	ack := readFrame(t, alice, protocol.FormatJSON, 2*time.Second)
	if ack.Action != protocol.ActionAck || len(ack.Res) == 0 || len(ack.Res[0].Serials) == 0 {
		t.Fatalf("create ack = %+v, want ACK with serials", ack)
	}
	serial := ack.Res[0].Serials[0]

	// own-allowed: alice updates her own message.
	if f := sendUpdate(t, alice, channel, serial, 2); f.Action != protocol.ActionAck {
		t.Errorf("alice update (own-allowed) = %+v, want ACK", f)
	}

	// own-denied: bob has message-update-own but is not the creator.
	bob := dialTokenClientID(t, srv, `{"chat:*":["message-update-own"]}`, "bob")
	drainConnected(t, bob)
	if f := sendUpdate(t, bob, channel, serial, 1); f.Action != protocol.ActionNack || f.Error == nil || f.Error.Code != 40160 {
		t.Errorf("bob update (own-denied) = %+v, want NACK 40160", f)
	}

	// any-allowed: carol has message-update-any.
	carol := dialTokenClientID(t, srv, `{"chat:*":["message-update-any"]}`, "carol")
	drainConnected(t, carol)
	if f := sendUpdate(t, carol, channel, serial, 1); f.Action != protocol.ActionAck {
		t.Errorf("carol update (any-allowed) = %+v, want ACK", f)
	}

	// missing-capability: dave has only publish.
	dave := dialTokenClientID(t, srv, `{"chat:*":["publish"]}`, "dave")
	drainConnected(t, dave)
	if f := sendUpdate(t, dave, channel, serial, 1); f.Action != protocol.ActionNack || f.Error == nil || f.Error.Code != 40160 {
		t.Errorf("dave update (missing-cap) = %+v, want NACK 40160", f)
	}
}
