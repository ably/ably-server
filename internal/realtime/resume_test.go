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

// dialResume connects with the test key and a resume/recover query param.
func dialResume(t *testing.T, srv *httptest.Server, param, value string) *websocket.Conn {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	u.Scheme = "ws"
	q := u.Query()
	q.Set("key", testKey)
	q.Set(param, value)
	u.RawQuery = q.Encode()

	ws, _, err := websocket.DefaultDialer.DialContext(context.Background(), u.String(), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = ws.Close() })
	return ws
}

// A valid, authenticated resume key retains the connectionId on the new
// connection — identity continuity doesn't require replaying any state
// (connection-state resume stays a non-goal, DESIGN.md §1/§8/§11).
func TestResumeWithValidKeyRetainsConnectionID(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	ws1 := dial(t, srv, "")
	first := readFrame(t, ws1, protocol.FormatJSON, 2*time.Second)
	if first.Action != protocol.ActionConnected || first.ConnectionDetails == nil {
		t.Fatalf("first connect = %+v, want CONNECTED with ConnectionDetails", first)
	}
	key := first.ConnectionDetails.ConnectionKey
	ws1.Close()

	ws2 := dialResume(t, srv, "resume", key)
	second := readFrame(t, ws2, protocol.FormatJSON, 2*time.Second)
	if second.Action != protocol.ActionConnected {
		t.Fatalf("resumed connect = %+v, want CONNECTED", second)
	}
	if second.ConnectionID != first.ConnectionID {
		t.Errorf("resumed connectionId = %q, want it to match the original %q", second.ConnectionID, first.ConnectionID)
	}
	if second.Error != nil {
		t.Errorf("resumed CONNECTED carried an error: %+v, want none", second.Error)
	}
}

// A resume/recover key that doesn't authenticate — a bare connectionId
// (e.g. one observed on a delivered Message) or a tampered one — is
// declined per protocol: a fresh connectionId, with 80018 on CONNECTED so
// the SDK knows the resume/recover failed (RTN15c7, RTN16e).
func TestResumeWithBareConnectionIDIsDeclined(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	ws1 := dial(t, srv, "")
	first := readFrame(t, ws1, protocol.FormatJSON, 2*time.Second)
	ws1.Close()

	// Present the bare connectionId (no HMAC suffix) as the resume key —
	// exactly what an attacker who only observed the connectionId would
	// have.
	ws2 := dialResume(t, srv, "resume", first.ConnectionID)
	second := readFrame(t, ws2, protocol.FormatJSON, 2*time.Second)
	if second.Action != protocol.ActionConnected {
		t.Fatalf("resumed connect = %+v, want CONNECTED", second)
	}
	if second.ConnectionID == first.ConnectionID {
		t.Errorf("bare connectionId resumed identity, want a fresh connectionId")
	}
	if second.Error == nil || second.Error.Code != 80018 {
		t.Errorf("error = %+v, want 80018", second.Error)
	}
}

// A tampered connectionKey (valid connectionId, wrong suffix) is declined
// the same way as a bare connectionId.
func TestResumeWithTamperedKeyIsDeclined(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	ws1 := dial(t, srv, "")
	first := readFrame(t, ws1, protocol.FormatJSON, 2*time.Second)
	key := first.ConnectionDetails.ConnectionKey
	ws1.Close()

	tampered := key[:len(key)-1] + "0"
	if tampered == key {
		tampered = key[:len(key)-1] + "1"
	}
	ws2 := dialResume(t, srv, "recover", tampered)
	second := readFrame(t, ws2, protocol.FormatJSON, 2*time.Second)
	if second.ConnectionID == first.ConnectionID {
		t.Errorf("tampered key resumed identity, want a fresh connectionId")
	}
	if second.Error == nil || second.Error.Code != 80018 {
		t.Errorf("error = %+v, want 80018", second.Error)
	}
}
