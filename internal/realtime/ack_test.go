package realtime

import (
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"

	"github.com/gorilla/websocket"
)

// collectAcks reads frames until it has seen wantAcks ACK/NACK frames,
// returning them in arrival order. Non-ACK frames (echoed MESSAGEs, etc.)
// are skipped.
func collectAcks(t *testing.T, ws *websocket.Conn, wantAcks int) []*protocol.ProtocolMessage {
	t.Helper()
	acks := make([]*protocol.ProtocolMessage, 0, wantAcks)
	deadline := time.Now().Add(3 * time.Second)
	for len(acks) < wantAcks && time.Now().Before(deadline) {
		f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
		if f.Action == protocol.ActionAck || f.Action == protocol.ActionNack {
			acks = append(acks, f)
		}
	}
	if len(acks) < wantAcks {
		t.Fatalf("collected %d ACK/NACK frames, want %d", len(acks), wantAcks)
	}
	return acks
}

// TestAckCountPerProtocolMessage reproduces the ably-go pending-emitter
// accounting sequence from TASK-33: a create then an update over ONE
// connection. ably-go acks queue[:count] FRAMES per ACK, panicking when
// count exceeds the pending frames. So every inbound protocol message must
// draw exactly one ACK with Count==1, and the acknowledged msgSerials must
// advance monotonically (one per sent frame). See DESIGN.md §8.
func TestAckCountPerProtocolMessage(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dialClient(t, srv, "alice")
	drainConnected(t, ws)
	attach(t, ws, "room", protocol.FlagPublish|protocol.FlagSubscribe)

	// Frame 1 (msgSerial 1): a create. Its echoed MESSAGE carries the
	// server-assigned serial we target with the update.
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "room",
		MsgSerial: msgSerialPtr(1),
		Messages:  []*protocol.Message{{Data: "v1"}},
	})

	// Drain frame 1's ACK and echoed MESSAGE (either order) to learn the
	// created serial.
	var createAck *protocol.ProtocolMessage
	var target string
	for range 2 {
		f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
		switch f.Action {
		case protocol.ActionAck, protocol.ActionNack:
			createAck = f
		case protocol.ActionMessage:
			target = f.Messages[0].Serial
		}
	}
	if target == "" {
		t.Fatal("never observed the created message serial")
	}

	// Frame 2 (msgSerial 2): an update targeting the create.
	sendMutation(t, ws, "room", 2, &protocol.Message{
		Action: protocol.MessageUpdate, Serial: target, Data: "v2",
	})
	updateAck := collectAcks(t, ws, 1)[0]

	// Exactly one ACK per frame, each Count==1, msgSerials 1 then 2 —
	// monotonically-increasing, one msgSerial per inbound protocol message.
	acks := []*protocol.ProtocolMessage{createAck, updateAck}
	for i, want := range []int64{1, 2} {
		a := acks[i]
		if a == nil {
			t.Fatalf("frame %d: no ACK observed", i+1)
		}
		if a.Action != protocol.ActionAck {
			t.Fatalf("frame %d: action = %v, want ACK", i+1, a.Action)
		}
		if a.Count != 1 {
			t.Errorf("frame %d (msgSerial %d): Count = %d, want 1", i+1, a.PublishSerial(), a.Count)
		}
		if a.PublishSerial() != want {
			t.Errorf("ACK %d: msgSerial = %d, want %d", i+1, a.PublishSerial(), want)
		}
	}
}

// TestAckCountMultiMessagePublish guards the multi-message publish path: a
// single MESSAGE frame carrying N>=2 messages is one protocol message, so
// it draws one ACK with Count==1 (not N) — the per-message serials ride the
// single Res entry (TR4s, DESIGN.md §8).
func TestAckCountMultiMessagePublish(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dialClient(t, srv, "alice")
	drainConnected(t, ws)
	attach(t, ws, "room", protocol.FlagPublish|protocol.FlagSubscribe)

	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "room",
		MsgSerial: msgSerialPtr(1),
		Messages: []*protocol.Message{
			{Data: "a"}, {Data: "b"}, {Data: "c"},
		},
	})

	ack := collectAcks(t, ws, 1)[0]
	if ack.Action != protocol.ActionAck {
		t.Fatalf("action = %v, want ACK", ack.Action)
	}
	if ack.Count != 1 {
		t.Errorf("Count = %d, want 1 (count is per protocol message, not per inner message)", ack.Count)
	}
	if len(ack.Res) != 1 {
		t.Fatalf("Res has %d entries, want 1", len(ack.Res))
	}
	if len(ack.Res[0].Serials) != 3 {
		t.Errorf("Res[0].Serials = %v, want 3 serials", ack.Res[0].Serials)
	}
}
