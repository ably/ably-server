package protocol

import (
	"reflect"
	"strings"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	original := &ProtocolMessage{
		Action:       ActionConnected,
		ConnectionID: "abc123def456",
		MsgSerial:    i64p(42),
		Timestamp:    1700000000000,
	}

	for _, f := range []Format{FormatJSON, FormatMsgpack} {
		t.Run(f.String(), func(t *testing.T) {
			data, err := Marshal(original, f)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if len(data) == 0 {
				t.Fatal("Marshal returned empty bytes")
			}

			var decoded ProtocolMessage
			if err := Unmarshal(data, f, &decoded); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if !reflect.DeepEqual(decoded, *original) {
				t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", decoded, *original)
			}
		})
	}
}

func TestHeartbeatAlwaysCarriesAction(t *testing.T) {
	hb := &ProtocolMessage{Action: ActionHeartbeat}
	data, err := Marshal(hb, FormatJSON)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if got := string(data); got != `{"action":0}` {
		t.Fatalf("heartbeat JSON = %q, want %q", got, `{"action":0}`)
	}
}

func TestUnmarshalIgnoresUnknownFields(t *testing.T) {
	data := []byte(`{"action":4,"connectionId":"x","unknownField":"ignored"}`)
	var m ProtocolMessage
	if err := Unmarshal(data, FormatJSON, &m); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if m.Action != ActionConnected {
		t.Errorf("Action = %v, want CONNECTED", m.Action)
	}
	if m.ConnectionID != "x" {
		t.Errorf("ConnectionID = %q, want %q", m.ConnectionID, "x")
	}
}

func TestFormatFromQuery(t *testing.T) {
	cases := []struct {
		in      string
		want    Format
		wantErr bool
	}{
		{"", FormatJSON, false},
		{"json", FormatJSON, false},
		{"msgpack", FormatMsgpack, false},
		{"protobuf", 0, true},
	}
	for _, tc := range cases {
		got, err := FormatFromQuery(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("FormatFromQuery(%q) expected error, got nil", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("FormatFromQuery(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("FormatFromQuery(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestMessageRoundTrip(t *testing.T) {
	original := &Message{
		ID:           "conn:1:0",
		ClientID:     "alice",
		ConnectionID: "conn",
		Name:         "greeting",
		Data:         "hello world",
		Encoding:     "utf-8",
		Timestamp:    1700000000000,
	}

	for _, f := range []Format{FormatJSON, FormatMsgpack} {
		t.Run(f.String(), func(t *testing.T) {
			// Wrap in a ProtocolMessage so we exercise the full publish
			// shape that lands on the wire.
			out := &ProtocolMessage{
				Action:   ActionMessage,
				Channel:  "foo",
				Messages: []*Message{original},
			}
			data, err := Marshal(out, f)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var decoded ProtocolMessage
			if err := Unmarshal(data, f, &decoded); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if len(decoded.Messages) != 1 {
				t.Fatalf("decoded Messages length = %d, want 1", len(decoded.Messages))
			}
			if !reflect.DeepEqual(decoded.Messages[0], original) {
				t.Fatalf("Message round-trip mismatch:\n got %+v\nwant %+v", decoded.Messages[0], original)
			}
		})
	}
}

func i64p(v int64) *int64 { return &v }

// TestAckCarriesMsgSerialZero pins TASK-97 AC#1: an ACK (and NACK) for the
// first publish on a connection — msgSerial 0 — must emit an explicit
// msgSerial field in BOTH JSON and msgpack. ably-js reads it positionally
// and computes NaN when it is absent, hanging the first publish.
func TestAckCarriesMsgSerialZero(t *testing.T) {
	ack := &ProtocolMessage{Action: ActionAck, MsgSerial: i64p(0), Count: 1}

	jsonData, err := Marshal(ack, FormatJSON)
	if err != nil {
		t.Fatalf("Marshal JSON: %v", err)
	}
	if !strings.Contains(string(jsonData), `"msgSerial":0`) {
		t.Errorf("ACK JSON %q missing literal %q", jsonData, `"msgSerial":0`)
	}

	mpData, err := Marshal(ack, FormatMsgpack)
	if err != nil {
		t.Fatalf("Marshal msgpack: %v", err)
	}
	var decoded map[string]any
	if err := unmarshalMsgpackMap(mpData, &decoded); err != nil {
		t.Fatalf("decode msgpack map: %v", err)
	}
	v, ok := decoded["msgSerial"]
	if !ok {
		t.Fatalf("ACK msgpack map %v missing msgSerial key", decoded)
	}
	if n, _ := toInt64(v); n != 0 {
		t.Errorf("ACK msgpack msgSerial = %v, want 0", v)
	}
}

// TestNonPublishFramesOmitMsgSerial pins the other half of TASK-97: frames
// the server never stamps with a msgSerial (HEARTBEAT, CONNECTED, and a
// MESSAGE delivery) must NOT carry a spurious msgSerial:0. pointer-nil +
// omitempty keeps them clean.
func TestNonPublishFramesOmitMsgSerial(t *testing.T) {
	frames := map[string]*ProtocolMessage{
		"heartbeat": {Action: ActionHeartbeat},
		"connected": {Action: ActionConnected, ConnectionID: "abc"},
		"message":   {Action: ActionMessage, Channel: "c", Messages: []*Message{{Serial: "s:000", Data: "x"}}},
	}
	for name, f := range frames {
		t.Run(name, func(t *testing.T) {
			data, err := Marshal(f, FormatJSON)
			if err != nil {
				t.Fatalf("Marshal JSON: %v", err)
			}
			if strings.Contains(string(data), "msgSerial") {
				t.Errorf("%s JSON %q unexpectedly carries msgSerial", name, data)
			}
			mp, err := Marshal(f, FormatMsgpack)
			if err != nil {
				t.Fatalf("Marshal msgpack: %v", err)
			}
			var decoded map[string]any
			if err := unmarshalMsgpackMap(mp, &decoded); err != nil {
				t.Fatalf("decode msgpack map: %v", err)
			}
			if _, ok := decoded["msgSerial"]; ok {
				t.Errorf("%s msgpack map %v unexpectedly carries msgSerial", name, decoded)
			}
		})
	}
}

func TestActionString(t *testing.T) {
	if got := ActionConnected.String(); got != "connected" {
		t.Errorf("ActionConnected.String() = %q, want %q", got, "connected")
	}
	if got := Action(99).String(); !strings.Contains(got, "unknown") {
		t.Errorf("unknown action string = %q, want contains %q", got, "unknown")
	}
}
