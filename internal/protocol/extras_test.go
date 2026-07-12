package protocol

import (
	"reflect"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// TestExtrasRoundTrip is TASK-105: a client-supplied extras object must be
// preserved verbatim through both wire formats (JSON, msgpack) and the
// msgpack storage payload encoding, for Message, PresenceMessage and
// Annotation. The map carrier round-trips both encodings; a raw-JSON
// carrier would not survive the msgpack storage path (DESIGN.md §8).
func TestExtrasRoundTrip(t *testing.T) {
	// Nested string-valued object mirrors the ably-js fixtures
	// ({headers:{some:metadata}}); string values keep DeepEqual stable
	// across msgpack's numeric-kind decoding.
	extras := map[string]any{
		"headers": map[string]any{"some": "metadata"},
		"push":    map[string]any{"notification": map[string]any{"title": "hi"}},
	}

	t.Run("Message/wire", func(t *testing.T) {
		for _, f := range []Format{FormatJSON, FormatMsgpack} {
			t.Run(f.String(), func(t *testing.T) {
				in := &ProtocolMessage{
					Action:   ActionMessage,
					Channel:  "c",
					Messages: []*Message{{Serial: "s:000", Action: MessageCreate, Data: "x", Extras: extras}},
				}
				wire, err := Marshal(in, f)
				if err != nil {
					t.Fatalf("Marshal: %v", err)
				}
				var out ProtocolMessage
				if err := Unmarshal(wire, f, &out); err != nil {
					t.Fatalf("Unmarshal: %v", err)
				}
				if len(out.Messages) != 1 {
					t.Fatalf("got %d messages, want 1", len(out.Messages))
				}
				if !reflect.DeepEqual(out.Messages[0].Extras, extras) {
					t.Errorf("extras round-trip mismatch:\n got %#v\nwant %#v", out.Messages[0].Extras, extras)
				}
			})
		}
	})

	t.Run("Message/storage-msgpack", func(t *testing.T) {
		// Storage marshals the Message value directly (bbolt/postgres).
		m := &Message{Serial: "s:000", Action: MessageCreate, Data: "x", Extras: extras}
		blob, err := msgpack.Marshal(m)
		if err != nil {
			t.Fatalf("msgpack Marshal: %v", err)
		}
		var got Message
		if err := msgpack.Unmarshal(blob, &got); err != nil {
			t.Fatalf("msgpack Unmarshal: %v", err)
		}
		if !reflect.DeepEqual(got.Extras, extras) {
			t.Errorf("storage extras round-trip mismatch:\n got %#v\nwant %#v", got.Extras, extras)
		}
	})

	t.Run("PresenceMessage", func(t *testing.T) {
		for _, f := range []Format{FormatJSON, FormatMsgpack} {
			t.Run(f.String(), func(t *testing.T) {
				in := &ProtocolMessage{
					Action:   ActionPresence,
					Channel:  "c",
					Presence: []*PresenceMessage{{Action: PresenceEnter, ClientID: "alice", Extras: extras}},
				}
				wire, err := Marshal(in, f)
				if err != nil {
					t.Fatalf("Marshal: %v", err)
				}
				var out ProtocolMessage
				if err := Unmarshal(wire, f, &out); err != nil {
					t.Fatalf("Unmarshal: %v", err)
				}
				if len(out.Presence) != 1 || !reflect.DeepEqual(out.Presence[0].Extras, extras) {
					t.Errorf("presence extras round-trip mismatch: got %#v", out.Presence)
				}
			})
		}
		// Storage payload path.
		p := &PresenceMessage{Action: PresenceEnter, ClientID: "alice", Extras: extras}
		blob, err := msgpack.Marshal(p)
		if err != nil {
			t.Fatalf("msgpack Marshal: %v", err)
		}
		var got PresenceMessage
		if err := msgpack.Unmarshal(blob, &got); err != nil {
			t.Fatalf("msgpack Unmarshal: %v", err)
		}
		if !reflect.DeepEqual(got.Extras, extras) {
			t.Errorf("presence storage extras mismatch:\n got %#v\nwant %#v", got.Extras, extras)
		}
	})

	t.Run("Annotation", func(t *testing.T) {
		for _, f := range []Format{FormatJSON, FormatMsgpack} {
			t.Run(f.String(), func(t *testing.T) {
				in := &ProtocolMessage{
					Action:      ActionAnnotation,
					Channel:     "c",
					Annotations: []*Annotation{{Action: AnnotationCreate, Type: "reaction:multiple.v1", MessageSerial: "t:000", Extras: extras}},
				}
				wire, err := Marshal(in, f)
				if err != nil {
					t.Fatalf("Marshal: %v", err)
				}
				var out ProtocolMessage
				if err := Unmarshal(wire, f, &out); err != nil {
					t.Fatalf("Unmarshal: %v", err)
				}
				if len(out.Annotations) != 1 || !reflect.DeepEqual(out.Annotations[0].Extras, extras) {
					t.Errorf("annotation extras round-trip mismatch: got %#v", out.Annotations)
				}
			})
		}
		// Storage payload path.
		a := &Annotation{Action: AnnotationCreate, Type: "reaction:multiple.v1", MessageSerial: "t:000", Extras: extras}
		blob, err := msgpack.Marshal(a)
		if err != nil {
			t.Fatalf("msgpack Marshal: %v", err)
		}
		var got Annotation
		if err := msgpack.Unmarshal(blob, &got); err != nil {
			t.Fatalf("msgpack Unmarshal: %v", err)
		}
		if !reflect.DeepEqual(got.Extras, extras) {
			t.Errorf("annotation storage extras mismatch:\n got %#v\nwant %#v", got.Extras, extras)
		}
	})
}
