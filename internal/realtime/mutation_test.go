package realtime

import (
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"

	"github.com/gorilla/websocket"
)

// publishCreate sends a single create on channel from an attachment that
// holds both PUBLISH and SUBSCRIBE, drains the ACK and the echoed
// MESSAGE, and returns the created message's stable serial (identity).
func publishCreate(t *testing.T, ws *websocket.Conn, channel string, msgSerial int64, data string) string {
	t.Helper()
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   channel,
		MsgSerial: msgSerial,
		Messages:  []*protocol.Message{{Data: data}},
	})
	var serial string
	for range 2 { // ACK + echoed MESSAGE race; either order
		f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
		if f.Action == protocol.ActionMessage && len(f.Messages) == 1 {
			serial = f.Messages[0].Serial
		}
	}
	if serial == "" {
		t.Fatal("publishCreate: never observed the created message serial")
	}
	return serial
}

// sendMutation sends a single mutation MESSAGE frame (no read).
func sendMutation(t *testing.T, ws *websocket.Conn, channel string, msgSerial int64, m *protocol.Message) {
	t.Helper()
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   channel,
		MsgSerial: msgSerial,
		Messages:  []*protocol.Message{m},
	})
}

// readMessage reads frames until a MESSAGE arrives (skipping ACKs).
func readMessage(t *testing.T, ws *websocket.Conn) *protocol.ProtocolMessage {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
		if f.Action == protocol.ActionMessage {
			return f
		}
	}
	t.Fatal("readMessage: no MESSAGE frame within deadline")
	return nil
}

// TestMutationUpdateAckedAndForwarded: an inbound update MESSAGE is
// persisted, ACKed on its msgSerial, and delivered to subscribers as an
// outbound MESSAGE carrying action=update, the new version, and the
// unchanged serial — reusing the MESSAGE frame, no new action (§13.6).
func TestMutationUpdateAckedAndForwarded(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dialClient(t, srv, "alice")
	drainConnected(t, ws)
	attach(t, ws, "room", protocol.FlagPublish|protocol.FlagSubscribe)

	target := publishCreate(t, ws, "room", 1, "v1")

	sendMutation(t, ws, "room", 2, &protocol.Message{
		Action: protocol.MessageUpdate, Serial: target, Data: "v2",
	})

	var ack, fwd *protocol.ProtocolMessage
	for range 2 {
		f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
		switch f.Action {
		case protocol.ActionAck:
			ack = f
		case protocol.ActionMessage:
			fwd = f
		}
	}
	if ack == nil || ack.MsgSerial != 2 || ack.Count != 1 {
		t.Fatalf("ACK = %+v, want msgSerial 2 / count 1", ack)
	}
	if fwd == nil {
		t.Fatal("no forwarded MESSAGE for the update")
	}
	if fwd.Action != protocol.ActionMessage {
		t.Errorf("forwarded frame action = %v, want MESSAGE (15, no new action)", fwd.Action)
	}
	m := fwd.Messages[0]
	if m.Action != protocol.MessageUpdate {
		t.Errorf("message action = %v, want update", m.Action)
	}
	if m.Serial != target {
		t.Errorf("serial = %q, want unchanged identity %q", m.Serial, target)
	}
	if m.Data != "v2" {
		t.Errorf("data = %v, want v2", m.Data)
	}
	if m.Version == nil || m.Version.Serial == target {
		t.Errorf("version = %+v, want a fresh version serial != identity", m.Version)
	}
}

// TestMutationDeleteForwarded: a delete is ACKed and delivered as an
// outbound MESSAGE with action=delete and the unchanged serial.
func TestMutationDeleteForwarded(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dialClient(t, srv, "alice")
	drainConnected(t, ws)
	attach(t, ws, "room", protocol.FlagPublish|protocol.FlagSubscribe)

	target := publishCreate(t, ws, "room", 1, "secret")
	sendMutation(t, ws, "room", 2, &protocol.Message{Action: protocol.MessageDelete, Serial: target})

	var ack, fwd *protocol.ProtocolMessage
	for range 2 {
		f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
		switch f.Action {
		case protocol.ActionAck:
			ack = f
		case protocol.ActionMessage:
			fwd = f
		}
	}
	if ack == nil || ack.MsgSerial != 2 {
		t.Fatalf("ACK = %+v, want msgSerial 2", ack)
	}
	if fwd == nil || fwd.Messages[0].Action != protocol.MessageDelete {
		t.Fatalf("forwarded = %+v, want a delete MESSAGE", fwd)
	}
	if fwd.Messages[0].Serial != target {
		t.Errorf("delete serial = %q, want %q", fwd.Messages[0].Serial, target)
	}
}

// TestMutationCrossSubscriberDelivery: an update published by one
// connection reaches a different subscribed connection in stream order.
func TestMutationCrossSubscriberDelivery(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	sub := dialClient(t, srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", protocol.FlagSubscribe)

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPublish|protocol.FlagSubscribe)

	target := publishCreate(t, pub, "room", 1, "v1")

	// Subscriber sees the create first.
	create := readMessage(t, sub)
	if create.Messages[0].Action != protocol.MessageCreate || create.Messages[0].Serial != target {
		t.Fatalf("subscriber create = %+v, want create with serial %q", create.Messages[0], target)
	}

	sendMutation(t, pub, "room", 2, &protocol.Message{Action: protocol.MessageUpdate, Serial: target, Data: "v2"})

	upd := readMessage(t, sub)
	if upd.Messages[0].Action != protocol.MessageUpdate {
		t.Errorf("subscriber mutation action = %v, want update", upd.Messages[0].Action)
	}
	if upd.Messages[0].Serial != target || upd.Messages[0].Data != "v2" {
		t.Errorf("subscriber mutation = %+v, want serial %q data v2", upd.Messages[0], target)
	}
}

// TestMutationTargetNotFound: a mutation against a serial that was never
// published is NACKed, not silently dropped.
func TestMutationTargetNotFound(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	ws := dialClient(t, srv, "alice")
	drainConnected(t, ws)
	attach(t, ws, "room", protocol.FlagPublish)

	sendMutation(t, ws, "room", 5, &protocol.Message{
		Action: protocol.MessageUpdate, Serial: "00000000000001-000@deadbeef00:000", Data: "x",
	})

	f := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if f.Action != protocol.ActionNack {
		t.Fatalf("action = %v, want NACK", f.Action)
	}
	if f.MsgSerial != 5 {
		t.Errorf("NACK msgSerial = %d, want 5", f.MsgSerial)
	}
}

// TestMutationWithoutAttachmentSucceeds: a mutation needs no attachment —
// like a create publish, it is a write to the channel stream. The
// publisher here never attaches; the mutation is still ACKed.
func TestMutationWithoutAttachmentSucceeds(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	// A subscriber (separate connection) so we can learn the create serial
	// and confirm delivery.
	sub := dialClient(t, srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", protocol.FlagSubscribe)

	// Publisher: never attaches, just publishes a create then mutates it.
	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "room",
		MsgSerial: 1,
		Messages:  []*protocol.Message{{Data: "v1"}},
	})
	if ack := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); ack.Action != protocol.ActionAck {
		t.Fatalf("create frame = %v, want ACK", ack.Action)
	}
	create := readMessage(t, sub)
	target := create.Messages[0].Serial

	sendMutation(t, pub, "room", 2, &protocol.Message{Action: protocol.MessageUpdate, Serial: target, Data: "v2"})
	if ack := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); ack.Action != protocol.ActionAck {
		t.Fatalf("mutation frame = %v, want ACK (no attachment required)", ack.Action)
	}
	upd := readMessage(t, sub)
	if upd.Messages[0].Action != protocol.MessageUpdate || upd.Messages[0].Data != "v2" {
		t.Errorf("delivered mutation = %+v, want update/v2", upd.Messages[0])
	}
}
