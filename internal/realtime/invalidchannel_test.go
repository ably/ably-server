package realtime

import (
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
)

// TestAttachInvalidChannelNameErrors: an ATTACH to an invalid channel name
// is answered with ERROR 40010 carrying the channel, and the connection
// stays usable afterwards. A long heartbeat keeps an idle
// HEARTBEAT from racing the reads.
func TestAttachInvalidChannelNameErrors(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	ws := dial(t, srv, "")
	drainConnected(t, ws)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: new(":hell"),
	})

	f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if f.Action != protocol.ActionError {
		t.Fatalf("frame = %v, want ERROR", f.Action)
	}
	if f.GetChannel() != ":hell" {
		t.Fatalf("ERROR channel = %q, want %q", f.GetChannel(), ":hell")
	}
	if f.Error == nil || f.Error.Code != 40010 || f.Error.StatusCode != 400 {
		t.Fatalf("ERROR = %+v, want code 40010 status 400", f.Error)
	}

	// The connection remains usable: a valid attach still succeeds.
	attach(t, ws, "ok", protocol.FlagSubscribe)
}

// TestPublishInvalidChannelNameNacks: a MESSAGE published to an invalid
// channel name is NACKed with 40010.
func TestPublishInvalidChannelNameNacks(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	ws := dial(t, srv, "")
	drainConnected(t, ws)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   new(":hell"),
		MsgSerial: msgSerialPtr(0),
		Messages:  []*protocol.Message{{Data: "x"}},
	})

	f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if f.Action != protocol.ActionNack {
		t.Fatalf("frame = %v, want NACK", f.Action)
	}
	if f.Error == nil || f.Error.Code != 40010 || f.Error.StatusCode != 400 {
		t.Fatalf("NACK error = %+v, want code 40010 status 400", f.Error)
	}
}
