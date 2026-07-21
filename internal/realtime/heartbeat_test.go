package realtime

import (
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
)

// TestHeartbeatEchoesID: an inbound HEARTBEAT with an id (connection.ping)
// is answered with a HEARTBEAT echoing that id, which is how the SDK
// correlates the ping response. A long heartbeat interval keeps
// an idle server HEARTBEAT from racing the read.
func TestHeartbeatEchoesID(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	ws := dial(t, srv, "")
	drainConnected(t, ws)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action: protocol.ActionHeartbeat,
		ID:     "ping-1",
	})

	resp := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if resp.Action != protocol.ActionHeartbeat {
		t.Fatalf("response Action = %v, want HEARTBEAT", resp.Action)
	}
	if resp.ID != "ping-1" {
		t.Fatalf("response ID = %q, want %q", resp.ID, "ping-1")
	}
}
