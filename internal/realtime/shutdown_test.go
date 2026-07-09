package realtime

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/memory"
)

// newShutdownServer exposes the realtime.Server and its Manager so tests
// can drive Shutdown directly and inspect channel state.
func newShutdownServer(t *testing.T, hb time.Duration) (*httptest.Server, *Server, *core.Manager) {
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
	return srv, rt, manager
}

// TestShutdownPacesDisconnects: Shutdown sends DISCONNECTED (action 6) to
// every live connection and spreads the closures across the grace window
// rather than firing them all at once (TASK-2 AC #1, DESIGN.md §11).
func TestShutdownPacesDisconnects(t *testing.T) {
	srv, rt, _ := newShutdownServer(t, time.Hour)

	const n = 4
	const window = 800 * time.Millisecond
	interval := window / n

	conns := make([]*websocket.Conn, n)
	for i := range conns {
		conns[i] = dialClient(t, srv, "")
		drainConnected(t, conns[i])
	}

	// One reader per connection records when its DISCONNECTED arrives.
	got := make(chan time.Time, n)
	for _, ws := range conns {
		go func(ws *websocket.Conn) {
			_ = ws.SetReadDeadline(time.Now().Add(window + 2*time.Second))
			for {
				_, data, err := ws.ReadMessage()
				if err != nil {
					return
				}
				var m protocol.ProtocolMessage
				if protocol.Unmarshal(data, protocol.FormatJSON, &m) != nil {
					continue
				}
				if m.Action == protocol.ActionDisconnected {
					got <- time.Now()
					return
				}
			}
		}(ws)
	}

	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	rt.Shutdown(ctx)

	times := make([]time.Time, 0, n)
	deadline := time.After(window + 2*time.Second)
	for len(times) < n {
		select {
		case ts := <-got:
			times = append(times, ts)
		case <-deadline:
			t.Fatalf("only %d/%d connections received DISCONNECTED", len(times), n)
		}
	}

	minT, maxT := times[0], times[0]
	for _, ts := range times {
		if ts.Before(minT) {
			minT = ts
		}
		if ts.After(maxT) {
			maxT = ts
		}
	}
	span := maxT.Sub(minT)
	// Even pacing over the window should spread the closures across roughly
	// (n-1)*interval; require at least ~1.5 intervals so an "all at once"
	// implementation (span ~0) fails clearly.
	if span < interval+interval/2 {
		t.Errorf("DISCONNECTED span = %v, want closures paced across the window (>= %v)", span, interval+interval/2)
	}
}

// TestShutdownSynthesisesPresenceLeave: the graceful shutdown close path
// still drives the connection-loop teardown, which synthesises presence
// LEAVEs — after Shutdown the member is gone from the channel's presence
// set (DESIGN.md §12.5, §11).
func TestShutdownSynthesisesPresenceLeave(t *testing.T) {
	srv, rt, manager := newShutdownServer(t, time.Hour)

	ws := dialClient(t, srv, "alice")
	drainConnected(t, ws)
	attach(t, ws, "room", protocol.FlagPresence|protocol.FlagPresenceSubscribe)

	// Enter presence and confirm the member is in the set before shutdown.
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionPresence,
		Channel:   "room",
		MsgSerial: 1,
		Presence:  []*protocol.PresenceMessage{{Action: protocol.PresenceEnter, Data: "hi"}},
	})
	if ack := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); ack.Action != protocol.ActionAck {
		t.Fatalf("enter frame = %v, want ACK", ack.Action)
	}

	ch, err := manager.GetChannel(context.Background(), "room")
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	if !waitMember(t, ch, "alice", true, time.Second) {
		t.Fatal("alice not present after ENTER")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	rt.Shutdown(ctx)

	// Teardown runs asynchronously as the connection loop exits; the
	// synthesised LEAVE should remove alice from the presence set.
	if waitMember(t, ch, "alice", false, 2*time.Second) {
		t.Fatal("alice still present after shutdown; teardown LEAVE not synthesised")
	}
}

// waitMember polls the channel's presence set until clientID's presence
// matches want, or the timeout elapses; it returns the final observed
// present state.
func waitMember(t *testing.T, ch *core.Channel, clientID string, want bool, within time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		members, _, err := ch.Members(context.Background())
		if err != nil {
			t.Fatalf("Members: %v", err)
		}
		found := false
		for _, m := range members {
			if m.ClientID == clientID {
				found = true
				break
			}
		}
		if found == want || time.Now().After(deadline) {
			return found
		}
		time.Sleep(10 * time.Millisecond)
	}
}
