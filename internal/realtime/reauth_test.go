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

// signToken mints an HS256 JWT with the given capability/clientId claims
// and a TTL from now, signed with the test key.
func signToken(t *testing.T, capability, clientID string, ttl time.Duration) string {
	t.Helper()
	now := time.Now()
	// exp carries sub-second precision (as MintToken does) so a sub-second
	// ttl is honoured exactly rather than truncated to a whole second — with
	// no exp leeway a truncated exp could land in the past at connect.
	claims := jwt.MapClaims{"iat": now.Unix(), "exp": float64(now.Add(ttl).UnixNano()) / float64(time.Second)}
	if capability != "" {
		claims["x-ably-capability"] = capability
	}
	if clientID != "" {
		claims["x-ably-clientId"] = clientID
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tok.Header["kid"] = "app.key"
	s, err := tok.SignedString([]byte("secret"))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return s
}

// dialWithToken connects a WebSocket client presenting token via the
// access_token query param.
func dialWithToken(t *testing.T, srv *httptest.Server, token string) *websocket.Conn {
	t.Helper()
	u, _ := url.Parse(srv.URL)
	u.Scheme = "ws"
	q := u.Query()
	q.Set("access_token", token)
	u.RawQuery = q.Encode()
	ws, _, err := websocket.DefaultDialer.DialContext(context.Background(), u.String(), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = ws.Close() })
	return ws
}

// A token with enough life left (within preExpiryWindow but above the
// no-warning margin) triggers an immediate AUTH prompt, and lapsing without
// re-auth disconnects with 40142.
func TestAuthPromptAndExpiry(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dialWithToken(t, srv, signToken(t, `{"*":["*"]}`, "", 7*time.Second))

	if f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionConnected {
		t.Fatalf("first frame = %v, want CONNECTED", f.Action)
	}
	// promptAt = exp - preExpiryWindow is in the past, so the AUTH prompt
	// fires immediately after connect.
	if f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionAuth {
		t.Fatalf("second frame = %v, want AUTH prompt", f.Action)
	}
	// With no re-auth, the connection is disconnected at expiry with the
	// token-expired code.
	f := readFrame(t, ws, protocol.FormatJSON, 9*time.Second)
	if f.Action != protocol.ActionDisconnected || f.Error == nil || f.Error.Code != 40142 {
		t.Fatalf("frame = %+v, want DISCONNECTED 40142", f)
	}
}

// A token whose remaining lifetime is below the no-warning margin is not
// prompted at all: the next frame after CONNECTED is the token-expired
// DISCONNECTED. This is the RTN22a path — the SDK reconnects with a fresh
// token rather than renewing inband, so prompting would be counterproductive.
func TestShortTokenExpiresWithoutPrompt(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dialWithToken(t, srv, signToken(t, `{"*":["*"]}`, "", 700*time.Millisecond))

	if f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionConnected {
		t.Fatalf("first frame = %v, want CONNECTED", f.Action)
	}
	// No AUTH prompt for a sub-margin token: the connection goes straight to
	// the token-expired DISCONNECTED.
	f := readFrame(t, ws, protocol.FormatJSON, 3*time.Second)
	if f.Action != protocol.ActionDisconnected || f.Error == nil || f.Error.Code != 40142 {
		t.Fatalf("frame = %+v, want DISCONNECTED 40142 with no prompt", f)
	}
}

// A successful inband AUTH updates the capability set and expiry and
// replies with CONNECTED, without dropping the connection.
func TestReauthSuccess(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	// Start subscribe-only, near expiry (above the no-warning margin so a
	// prompt fires immediately).
	ws := dialWithToken(t, srv, signToken(t, `{"chat:*":["subscribe"]}`, "alice", 7*time.Second))
	if f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionConnected {
		t.Fatalf("first frame = %v, want CONNECTED", f.Action)
	}
	if f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionAuth {
		t.Fatalf("expected AUTH prompt, got %v", f.Action)
	}

	// Re-auth with a long-lived token that also grants publish.
	fresh := signToken(t, `{"chat:*":["subscribe","publish"]}`, "alice", time.Hour)
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action: protocol.ActionAuth,
		Auth:   &protocol.AuthDetails{AccessToken: fresh},
	})
	f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if f.Action != protocol.ActionConnected || f.ConnectionDetails == nil {
		t.Fatalf("re-auth reply = %+v, want CONNECTED with ConnectionDetails", f)
	}

	// The widened capability now permits publishing (previously NACKed),
	// and the connection survives past the original expiry.
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "chat:room",
		MsgSerial: msgSerialPtr(1),
		Messages:  []*protocol.Message{{Name: "n", Data: "x"}},
	})
	if f := readFrame(t, ws, protocol.FormatJSON, 3*time.Second); f.Action != protocol.ActionAck {
		t.Fatalf("publish after re-auth = %+v, want ACK", f)
	}
}

// An inband AUTH whose token resolves to a different clientId is rejected.
// A client-supplied credential the server rejects is not renewable, so the
// rejection is an ERROR frame (non-renewable code) that moves the SDK's
// connection to FAILED — not a renewable DISCONNECTED (RTC8a2).
func TestReauthIncompatibleClientID(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dialWithToken(t, srv, signToken(t, `{"*":["*"]}`, "alice", time.Hour))
	if f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionConnected {
		t.Fatalf("first frame = %v, want CONNECTED", f.Action)
	}

	// A far-future token so no prompt fires; supply a mismatched clientId.
	other := signToken(t, `{"*":["*"]}`, "bob", time.Hour)
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action: protocol.ActionAuth,
		Auth:   &protocol.AuthDetails{AccessToken: other},
	})
	f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if f.Action != protocol.ActionError || f.Error == nil || f.Error.Code != 40102 {
		t.Fatalf("frame = %+v, want ERROR 40102", f)
	}
}

// An inband AUTH carrying an unverifiable token is rejected with an ERROR
// frame carrying a non-renewable credential error (RTC8a2): the SDK moves the
// connection to FAILED rather than reconnecting, because the bad token was
// supplied by the client, not lapsed on the server's clock.
func TestReauthInvalidToken(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dialWithToken(t, srv, signToken(t, `{"*":["*"]}`, "alice", time.Hour))
	if f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionConnected {
		t.Fatalf("first frame = %v, want CONNECTED", f.Action)
	}

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action: protocol.ActionAuth,
		Auth:   &protocol.AuthDetails{AccessToken: "not.a.valid.token"},
	})
	f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if f.Action != protocol.ActionError || f.Error == nil || f.Error.Code != 40101 {
		t.Fatalf("frame = %+v, want ERROR 40101", f)
	}
}

// An inband AUTH with no access token is rejected with a non-renewable ERROR.
func TestReauthMissingToken(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dialWithToken(t, srv, signToken(t, `{"*":["*"]}`, "alice", time.Hour))
	if f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionConnected {
		t.Fatalf("first frame = %v, want CONNECTED", f.Action)
	}

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{Action: protocol.ActionAuth})
	f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if f.Action != protocol.ActionError || f.Error == nil || f.Error.Code != 40101 {
		t.Fatalf("frame = %+v, want ERROR 40101", f)
	}
}
