package realtime

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/memory"
)

// newPresenceGraceServer builds a realtime server with a shortened presence
// grace window (DESIGN.md §12.5) so the delayed-leave path can be exercised
// without a real 15s wait, and returns the *Server so tests can read it back.
func newPresenceGraceServer(t *testing.T, grace time.Duration) (*httptest.Server, *Server) {
	t.Helper()
	parsed, err := auth.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("parse api key: %v", err)
	}
	manager := core.NewManager(memory.New(memory.Options{}))
	rt := NewServer([]auth.APIKey{parsed}, manager, time.Hour, logging.New(slog.DiscardHandler), nil, nil)
	rt.remainPresentFor = grace
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rt.HandleWebSocket)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, rt
}

// dialResumeClientID connects with the test key, a resume key, and a clientId
// — a resumed connection that re-enters presence as the same member.
func dialResumeClientID(t *testing.T, srv *httptest.Server, resumeKey, clientID string) *websocket.Conn {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	u.Scheme = "ws"
	q := u.Query()
	q.Set("key", testKey)
	q.Set("resume", resumeKey)
	q.Set("clientId", clientID)
	u.RawQuery = q.Encode()
	ws, _, err := websocket.DefaultDialer.DialContext(context.Background(), u.String(), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = ws.Close() })
	return ws
}

// TestPresenceDelayedLeaveOnAbruptDisconnect: when a presence member's
// connection drops abruptly (no clean CLOSE), the LEAVE is not synthesised
// immediately — it is delayed by remainPresentFor, then delivered
// (DESIGN.md §12.5).
func TestPresenceDelayedLeaveOnAbruptDisconnect(t *testing.T) {
	const grace = 400 * time.Millisecond
	srv, _ := newPresenceGraceServer(t, grace)

	sub := dial(t, srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", 0) // full modes incl. PRESENCE_SUBSCRIBE

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPresence)
	enter(t, pub, "room", 1)

	// Subscriber sees the ENTER.
	if fwd := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); fwd.Action != protocol.ActionPresence || fwd.Presence[0].Action != protocol.PresenceEnter {
		t.Fatalf("first frame = %+v, want presence ENTER", fwd)
	}

	// Abrupt drop: close the transport without an Ably CLOSE frame.
	dropped := time.Now()
	_ = pub.Close()

	// The LEAVE must arrive, but only after the grace window.
	fwd := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	elapsed := time.Since(dropped)
	if fwd.Action != protocol.ActionPresence || len(fwd.Presence) != 1 || fwd.Presence[0].Action != protocol.PresenceLeave {
		t.Fatalf("frame = %+v, want a presence LEAVE", fwd)
	}
	if fwd.Presence[0].ClientID != "alice" {
		t.Errorf("leave clientId = %q, want alice", fwd.Presence[0].ClientID)
	}
	if elapsed < grace-100*time.Millisecond {
		t.Errorf("LEAVE arrived after %v, want it delayed by ~%v (not immediate)", elapsed, grace)
	}
}

// TestPresenceImmediateLeaveOnCleanClose: a clean client CLOSE is an
// intentional departure — the LEAVE is synthesised immediately, with no
// grace delay (DESIGN.md §12.5).
func TestPresenceImmediateLeaveOnCleanClose(t *testing.T) {
	const grace = 3 * time.Second
	srv, _ := newPresenceGraceServer(t, grace)

	sub := dial(t, srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", 0)

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPresence)
	enter(t, pub, "room", 1)

	if fwd := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); fwd.Presence[0].Action != protocol.PresenceEnter {
		t.Fatalf("first frame = %+v, want ENTER", fwd)
	}

	// Clean CLOSE handshake.
	closed := time.Now()
	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{Action: protocol.ActionClose})
	if f := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); f.Action != protocol.ActionClosed {
		t.Fatalf("expected CLOSED, got %v", f.Action)
	}
	_ = pub.Close()

	fwd := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	elapsed := time.Since(closed)
	if fwd.Action != protocol.ActionPresence || fwd.Presence[0].Action != protocol.PresenceLeave {
		t.Fatalf("frame = %+v, want a presence LEAVE", fwd)
	}
	if elapsed > grace/2 {
		t.Errorf("LEAVE arrived after %v, want it immediate (grace is %v)", elapsed, grace)
	}
}

// TestPresenceGraceSuppressedByResumeReenter: if the dropped connection
// resumes (same connectionId) and re-enters presence within the grace
// window, the scheduled LEAVE is suppressed — the member never flickers out
// (DESIGN.md §12.5).
func TestPresenceGraceSuppressedByResumeReenter(t *testing.T) {
	// Grace must comfortably outlast the resume round-trip (new WS
	// handshake + attach + re-enter) so the re-enter supersedes the member
	// before the scheduled leave's deadline; otherwise the leave fires
	// first and there is nothing to suppress.
	const grace = 2 * time.Second
	srv, _ := newPresenceGraceServer(t, grace)

	sub := dial(t, srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", 0)

	pub := dialClient(t, srv, "alice")
	connected := readFrame(t, pub, protocol.FormatJSON, 2*time.Second)
	if connected.Action != protocol.ActionConnected || connected.ConnectionDetails == nil {
		t.Fatalf("first frame = %+v, want CONNECTED with details", connected)
	}
	resumeKey := connected.ConnectionDetails.ConnectionKey
	attach(t, pub, "room", protocol.FlagPresence)
	enter(t, pub, "room", 1)

	if fwd := readFrame(t, sub, protocol.FormatJSON, 2*time.Second); fwd.Presence[0].Action != protocol.PresenceEnter {
		t.Fatalf("first sub frame = %+v, want ENTER", fwd)
	}

	// Abrupt drop, then resume (same connectionId) and re-enter within grace.
	_ = pub.Close()
	pub2 := dialResumeClientID(t, srv, resumeKey, "alice")
	resumed := readFrame(t, pub2, protocol.FormatJSON, 2*time.Second)
	if resumed.ConnectionID != connected.ConnectionID {
		t.Fatalf("resumed connectionId = %q, want retained %q", resumed.ConnectionID, connected.ConnectionID)
	}
	// Re-enter with a fresh msgSerial: a resumed connection continues its
	// msgSerial counter, so the re-enter mints a distinct presence id (a
	// repeat of msgSerial 1 would collide with the original enter's id and
	// be deduped as idempotent).
	attach(t, pub2, "room", protocol.FlagPresence)
	enter(t, pub2, "room", 2)

	// Subscriber sees the re-ENTER (same clientId, same connectionId).
	reenter := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if reenter.Action != protocol.ActionPresence || reenter.Presence[0].Action != protocol.PresenceEnter {
		t.Fatalf("re-enter frame = %+v, want ENTER", reenter)
	}

	// The scheduled LEAVE for the original connection must be suppressed:
	// no further presence frame within a window past the grace deadline.
	// (Terminal read — the deadline breaks the connection.)
	if err := sub.SetReadDeadline(time.Now().Add(grace)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, data, err := sub.ReadMessage(); err == nil {
		var f protocol.ProtocolMessage
		_ = protocol.Unmarshal(data, protocol.FormatJSON, &f)
		t.Fatalf("unexpected frame after resume+reenter: %+v (a LEAVE should have been suppressed)", f)
	}
}
