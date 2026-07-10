package realtime

import (
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/ably-server/internal/protocol"
)

// publishMessageForTarget publishes one message on channel and returns the
// server-assigned serial from the ACK's Res — a target to annotate.
func publishMessageForTarget(t *testing.T, ws *websocket.Conn, channel string, msgSerial int64) string {
	t.Helper()
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   channel,
		MsgSerial: msgSerial,
		Messages:  []*protocol.Message{{Data: "post"}},
	})
	ack := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if ack.Action != protocol.ActionAck {
		t.Fatalf("publish: frame = %v, want ACK", ack.Action)
	}
	if len(ack.Res) == 0 || len(ack.Res[0].Serials) == 0 {
		t.Fatalf("publish ACK missing Res serials: %+v", ack.Res)
	}
	return ack.Res[0].Serials[0]
}

// sendAnnotation sends one ANNOTATION frame targeting messageSerial.
func sendAnnotation(t *testing.T, ws *websocket.Conn, channel, messageSerial string, msgSerial int64) {
	t.Helper()
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionAnnotation,
		Channel:   channel,
		MsgSerial: msgSerial,
		Annotations: []*protocol.Annotation{{
			Action:        protocol.AnnotationCreate,
			Type:          "reaction:multiple.v1",
			Name:          "👍",
			MessageSerial: messageSerial,
		}},
	})
}

// TestAnnotationDeliveredToSubscriber: a publisher holding ANNOTATION_PUBLISH
// annotates a message, the publish is ACKed, and a peer holding
// ANNOTATION_SUBSCRIBE receives it as an ANNOTATION frame (TASK-64 AC#1,#2).
func TestAnnotationDeliveredToSubscriber(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPublish|protocol.FlagAnnotationPublish)
	target := publishMessageForTarget(t, pub, "room", 1)

	sub := dialClient(t, srv, "bob")
	drainConnected(t, sub)
	attach(t, sub, "room", protocol.FlagSubscribe|protocol.FlagAnnotationSubscribe)

	sendAnnotation(t, pub, "room", target, 2)
	if ack := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); ack.Action != protocol.ActionAck {
		t.Fatalf("annotation: frame = %v, want ACK", ack.Action)
	}

	got := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if got.Action != protocol.ActionAnnotation {
		t.Fatalf("subscriber frame = %v, want ANNOTATION", got.Action)
	}
	if len(got.Annotations) != 1 {
		t.Fatalf("ANNOTATION carried %d annotations, want 1", len(got.Annotations))
	}
	a := got.Annotations[0]
	if a.MessageSerial != target {
		t.Errorf("annotation messageSerial = %q, want target %q", a.MessageSerial, target)
	}
	if a.ClientID != "alice" {
		t.Errorf("annotation clientId = %q, want stamped alice", a.ClientID)
	}
	if a.ConnectionID == "" || a.Serial == "" {
		t.Errorf("annotation missing server-stamped connectionId/serial: %+v", a)
	}
}

// TestAnnotationNackWithoutPublishMode: an ANNOTATION from an attachment
// lacking ANNOTATION_PUBLISH is NACKed 40160 (TASK-64 AC#3).
func TestAnnotationNackWithoutPublishMode(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	// Attach with PUBLISH (to create a target) but NOT ANNOTATION_PUBLISH.
	// No SUBSCRIBE, so the publish is not echoed back to confuse the NACK read.
	attach(t, pub, "room", protocol.FlagPublish)
	target := publishMessageForTarget(t, pub, "room", 1)

	sendAnnotation(t, pub, "room", target, 2)
	nack := readFrame(t, pub, protocol.FormatJSON, 2*time.Second)
	if nack.Action != protocol.ActionNack {
		t.Fatalf("frame = %v, want NACK", nack.Action)
	}
	if nack.Error == nil || nack.Error.Code != 40160 {
		t.Errorf("NACK error = %+v, want code 40160", nack.Error)
	}
}

// TestAnnotationNackUnknownTarget: annotating a message that does not exist
// is NACKed 40400 (TASK-64 AC#1 target validation, §14.1).
func TestAnnotationNackUnknownTarget(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagAnnotationPublish)

	sendAnnotation(t, pub, "room", "00000000000001-000@nope:000", 1)
	nack := readFrame(t, pub, protocol.FormatJSON, 2*time.Second)
	if nack.Action != protocol.ActionNack {
		t.Fatalf("frame = %v, want NACK", nack.Action)
	}
	if nack.Error == nil || nack.Error.Code != 40400 {
		t.Errorf("NACK error = %+v, want code 40400", nack.Error)
	}
}

// TestAnnotationNotDeliveredWithoutSubscribeMode: a subscriber without
// ANNOTATION_SUBSCRIBE never sees ANNOTATION frames; a subsequent ordinary
// message arrives first, proving the annotation was skipped (TASK-64 AC#2).
func TestAnnotationNotDeliveredWithoutSubscribeMode(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPublish|protocol.FlagAnnotationPublish)
	target := publishMessageForTarget(t, pub, "room", 1)

	// SUBSCRIBE only — no ANNOTATION_SUBSCRIBE.
	sub := dialClient(t, srv, "bob")
	drainConnected(t, sub)
	attach(t, sub, "room", protocol.FlagSubscribe)

	sendAnnotation(t, pub, "room", target, 2)
	if ack := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); ack.Action != protocol.ActionAck {
		t.Fatalf("annotation ACK = %v", ack.Action)
	}
	// Now publish an ordinary message; the subscriber's first frame must be
	// that MESSAGE, not the earlier ANNOTATION.
	sendFrame(t, pub, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:    protocol.ActionMessage,
		Channel:   "room",
		MsgSerial: 3,
		Messages:  []*protocol.Message{{Data: "after"}},
	})
	if ack := readFrame(t, pub, protocol.FormatJSON, 2*time.Second); ack.Action != protocol.ActionAck {
		t.Fatalf("message ACK = %v", ack.Action)
	}
	got := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if got.Action != protocol.ActionMessage {
		t.Fatalf("subscriber first frame = %v, want MESSAGE (annotation must be skipped)", got.Action)
	}
	if len(got.Messages) == 0 || got.Messages[0].Data != "after" {
		t.Errorf("subscriber got %+v, want the 'after' message", got.Messages)
	}
}
