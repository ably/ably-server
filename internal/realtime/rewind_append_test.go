package realtime

import (
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
)

// TestRewindReplaysCollapsedAppendAggregate: a fresh subscriber attaching
// with rewind:1 after a message was created and then appended to receives
// the collapsed aggregate (action=update, the rolled-up data) — the backlog
// view of an append-updated message. Pins that a real rewind attach surfaces
// the latest-version projection of an appended message (DESIGN.md §13.3/§13.4).
func TestRewindReplaysCollapsedAppendAggregate(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPublish|protocol.FlagSubscribe)

	target := publishCreate(t, pub, "room", 1, "Hello")
	sendMutation(t, pub, "room", 2, &protocol.Message{Action: protocol.MessageAppend, Serial: target, Data: " World"})
	readMessage(t, pub) // drain append echo so it is linked on the live list

	attached, replayed := rewindAttachAndDrain(t, srv, "room", "1")
	if attached.Error != nil {
		t.Fatalf("ATTACHED.Error = %+v, want nil", attached.Error)
	}
	if len(replayed) != 1 {
		t.Fatalf("replayed len = %d, want 1 (the collapsed aggregate)", len(replayed))
	}
	m := replayed[0].Messages[0]
	if m.Action != protocol.MessageUpdate || m.Data != "Hello World" || m.Serial != target {
		t.Errorf("replayed = %+v, want update %q on %q", m, "Hello World", target)
	}
}
