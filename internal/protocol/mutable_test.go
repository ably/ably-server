package protocol

import (
	"reflect"
	"strings"
	"testing"
)

// TestMessageActionConstants pins the wire values to Ably's MessageAction
// enum (DESIGN.md §13.1). A change here is a protocol break.
func TestMessageActionConstants(t *testing.T) {
	cases := []struct {
		action MessageAction
		value  int8
		name   string
	}{
		{MessageCreate, 0, "create"},
		{MessageUpdate, 1, "update"},
		{MessageDelete, 2, "delete"},
		{MessageAppend, 5, "append"},
	}
	for _, tc := range cases {
		if int8(tc.action) != tc.value {
			t.Errorf("%s value = %d, want %d", tc.name, int8(tc.action), tc.value)
		}
		if got := tc.action.String(); got != tc.name {
			t.Errorf("%d.String() = %q, want %q", tc.value, got, tc.name)
		}
	}
	if got := MessageAction(99).String(); !strings.Contains(got, "unknown") {
		t.Errorf("unknown action string = %q, want contains %q", got, "unknown")
	}
}

func TestMessageActionIsMutation(t *testing.T) {
	if MessageCreate.IsMutation() {
		t.Error("create reported as a mutation")
	}
	for _, a := range []MessageAction{MessageUpdate, MessageDelete, MessageAppend} {
		if !a.IsMutation() {
			t.Errorf("%s not reported as a mutation", a)
		}
	}
}

// TestCreateActionEmittedOnWire guards §13.1: action=0 must be
// present on the wire (no omitempty), so SDKs that require the field
// always see it.
func TestCreateActionEmittedOnWire(t *testing.T) {
	data, err := Marshal(&ProtocolMessage{
		Action:   ActionMessage,
		Channel:  new("foo"),
		Messages: []*Message{{Serial: "s:000", Data: "x"}},
	}, FormatJSON)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(data), `"action":0`) {
		t.Errorf("create JSON %q missing %q", data, `"action":0`)
	}
}

// TestCreateMessageVersionRoundTrip exercises the create shape:
// version.serial == message serial (DESIGN.md §13.1 AC#4), through both
// codecs.
func TestCreateMessageVersionRoundTrip(t *testing.T) {
	serial := "00000000000001-000@abcdef0123:000"
	original := &Message{
		Serial:   serial,
		Action:   MessageCreate,
		ClientID: "alice",
		Name:     "greeting",
		Data:     "hello",
		Version: &MessageVersion{
			Serial:    serial, // create: version == serial
			Timestamp: 1700000000000,
			ClientID:  "alice",
		},
	}
	roundTrip(t, original)
}

// TestMutationMessageVersionRoundTrip exercises a mutation shape: the
// stable Serial is the target identity, the Version names the new
// position and carries operator metadata (DESIGN.md §13.1).
func TestMutationMessageVersionRoundTrip(t *testing.T) {
	target := "00000000000001-000@abcdef0123:000"
	original := &Message{
		Serial:   target, // unchanged across versions — the identity
		Action:   MessageUpdate,
		ClientID: "alice", // creator carried forward
		Data:     "edited",
		Version: &MessageVersion{
			Serial:      "00000000000002-000@abcdef0123:000", // new position
			Timestamp:   1700000005000,
			ClientID:    "bob", // operator differs from creator
			Description: "fixed a typo",
			Metadata:    map[string]any{"source": "moderation"},
		},
	}
	roundTrip(t, original)
}

func roundTrip(t *testing.T, original *Message) {
	t.Helper()
	for _, f := range []Format{FormatJSON, FormatMsgpack} {
		t.Run(f.String(), func(t *testing.T) {
			out := &ProtocolMessage{Action: ActionMessage, Channel: new("c"), Messages: []*Message{original}}
			data, err := Marshal(out, f)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var decoded ProtocolMessage
			if err := Unmarshal(data, f, &decoded); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if len(decoded.Messages) != 1 {
				t.Fatalf("decoded Messages = %d, want 1", len(decoded.Messages))
			}
			if !reflect.DeepEqual(decoded.Messages[0], original) {
				t.Fatalf("round-trip mismatch:\n got %+v (version %+v)\nwant %+v (version %+v)",
					decoded.Messages[0], decoded.Messages[0].Version, original, original.Version)
			}
		})
	}
}
