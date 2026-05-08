package protocol

import (
	"strings"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	original := &ProtocolMessage{
		Action:       ActionConnected,
		ConnectionID: "abc123def456",
		MsgSerial:    42,
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
			if decoded != *original {
				t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", decoded, *original)
			}
		})
	}
}

func TestHeartbeatJSONIsCompact(t *testing.T) {
	hb := &ProtocolMessage{Action: ActionHeartbeat}
	data, err := Marshal(hb, FormatJSON)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// Action 0 (HEARTBEAT) is omitempty, so the JSON form is just `{}`.
	if got := string(data); got != "{}" {
		t.Fatalf("heartbeat JSON = %q, want %q", got, "{}")
	}

	var decoded ProtocolMessage
	if err := Unmarshal(data, FormatJSON, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Action != ActionHeartbeat {
		t.Fatalf("decoded.Action = %v, want HEARTBEAT", decoded.Action)
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

func TestActionString(t *testing.T) {
	if got := ActionConnected.String(); got != "connected" {
		t.Errorf("ActionConnected.String() = %q, want %q", got, "connected")
	}
	if got := Action(99).String(); !strings.Contains(got, "unknown") {
		t.Errorf("unknown action string = %q, want contains %q", got, "unknown")
	}
}
