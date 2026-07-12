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
		MsgSerial: msgSerialPtr(msgSerial),
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
		MsgSerial: msgSerialPtr(msgSerial),
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

	// ANNOTATION_SUBSCRIBE only — this isolates the raw ANNOTATION frame
	// (a dual-mode subscriber would also get a summary MESSAGE; covered by
	// TestDualModeSubscriberGetsSummaryAndRawAnnotation).
	sub := dialClient(t, srv, "bob")
	drainConnected(t, sub)
	attach(t, sub, "room", protocol.FlagAnnotationSubscribe)

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

// TestSubscribeModeGetsSummaryNotRawAnnotation: a subscriber holding only
// SUBSCRIBE never sees a raw ANNOTATION frame — instead it receives the
// annotation as a MESSAGE with action summary (4) carrying the target's
// unchanged serial and the folded summary (DESIGN.md §14.3, TASK-66 AC#1,#5).
func TestSubscribeModeGetsSummaryNotRawAnnotation(t *testing.T) {
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

	got := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if got.Action != protocol.ActionMessage {
		t.Fatalf("subscriber frame = %v, want MESSAGE (summary), never a raw ANNOTATION", got.Action)
	}
	if len(got.Messages) != 1 {
		t.Fatalf("summary frame carried %d messages, want 1", len(got.Messages))
	}
	m := got.Messages[0]
	if m.Action != protocol.MessageSummary {
		t.Errorf("message action = %v, want summary (4)", m.Action)
	}
	if m.Serial != target {
		t.Errorf("summary serial = %q, want target %q (unchanged)", m.Serial, target)
	}
	agg := m.Summary["reaction:multiple.v1"]
	if agg == nil || agg.Counts["👍"] == nil || agg.Counts["👍"].Total != 1 {
		t.Errorf("summary = %#v, want reaction:multiple.v1 👍 total 1", m.Summary)
	}
	if agg != nil && agg.Counts["👍"] != nil && agg.Counts["👍"].ClientIDs["alice"] != 1 {
		t.Errorf("summary clientIds = %#v, want alice:1", agg.Counts["👍"].ClientIDs)
	}
}

// TestDualModeSubscriberGetsSummaryAndRawAnnotation: an attachment holding
// both SUBSCRIBE and ANNOTATION_SUBSCRIBE receives both frames from one
// annotation — the summary MESSAGE and the raw ANNOTATION (DESIGN.md §14.3).
func TestDualModeSubscriberGetsSummaryAndRawAnnotation(t *testing.T) {
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
		t.Fatalf("annotation ACK = %v", ack.Action)
	}

	// The summary MESSAGE is sent before the raw ANNOTATION (forward order).
	first := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if first.Action != protocol.ActionMessage || len(first.Messages) != 1 || first.Messages[0].Action != protocol.MessageSummary {
		t.Fatalf("first frame = %+v, want a summary MESSAGE", first)
	}
	second := readFrame(t, sub, protocol.FormatJSON, 2*time.Second)
	if second.Action != protocol.ActionAnnotation || len(second.Annotations) != 1 {
		t.Fatalf("second frame = %+v, want a raw ANNOTATION", second)
	}
	// The raw annotation must NOT leak the server-internal summary snapshot.
	if second.Annotations[0].Summary != nil {
		t.Errorf("raw ANNOTATION leaked summary snapshot: %#v", second.Annotations[0].Summary)
	}
}
