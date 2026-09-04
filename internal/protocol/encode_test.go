package protocol

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/ably/server-protocol/go/wire"
	"github.com/vmihailenco/msgpack/v5"
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

// TestExplicitEmptyChannelEmitted pins the pointer-Channel behaviour: an
// ERROR frame rejecting an empty channel name (Channel = pointer-to-"") must
// carry `channel:""` on the wire (both formats) so SDKs route it as a
// channel-scoped failure rather than a connection-fatal one, while a frame
// with a nil Channel omits the field entirely.
func TestExplicitEmptyChannelEmitted(t *testing.T) {
	forced := &ProtocolMessage{
		Action:  ActionError,
		Channel: new(""),
		Error:   &ErrorInfo{Code: 40010, StatusCode: 400, Message: "invalid channel name"},
	}
	// JSON: the empty channel is present as "".
	data, err := Marshal(forced, FormatJSON)
	if err != nil {
		t.Fatalf("Marshal JSON: %v", err)
	}
	if !strings.Contains(string(data), `"channel":""`) {
		t.Errorf("forced JSON = %q, want it to contain \"channel\":\"\"", data)
	}

	// Both formats decode back to an empty-but-present channel.
	for _, f := range []Format{FormatJSON, FormatMsgpack} {
		t.Run(f.String(), func(t *testing.T) {
			data, err := Marshal(forced, f)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			present, err := channelFieldPresent(data, f)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !present {
				t.Errorf("forced %s frame dropped the channel field: %q", f, data)
			}
		})
	}

	// Control: the same frame without the flag omits the empty channel, and
	// the default marshalling path is otherwise unchanged.
	plain := &ProtocolMessage{Action: ActionError, Error: forced.Error}
	for _, f := range []Format{FormatJSON, FormatMsgpack} {
		t.Run("omitted/"+f.String(), func(t *testing.T) {
			data, err := Marshal(plain, f)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			present, err := channelFieldPresent(data, f)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if present {
				t.Errorf("plain %s frame unexpectedly carries a channel field: %q", f, data)
			}
		})
	}
}

// channelFieldPresent reports whether the encoded frame carries a "channel"
// key at all (as distinct from carrying it with an empty value).
func channelFieldPresent(data []byte, f Format) (bool, error) {
	var m map[string]any
	switch f {
	case FormatJSON:
		if err := json.Unmarshal(data, &m); err != nil {
			return false, err
		}
	case FormatMsgpack:
		if err := msgpack.Unmarshal(data, &m); err != nil {
			return false, err
		}
	}
	_, ok := m["channel"]
	return ok, nil
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

func i64p(v int64) *int64 { return &v }

// TestAckCarriesMsgSerialZero pins the msgSerial:0 ACK case: an ACK (and NACK) for the
// first publish on a connection — msgSerial 0 — must emit an explicit
// msgSerial field in BOTH JSON and msgpack. SDKs read it positionally
// and compute NaN when it is absent, hanging the first publish.
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

// TestNonPublishFramesOmitMsgSerial pins the complementary case: frames
// the server never stamps with a msgSerial (HEARTBEAT, CONNECTED, and a
// MESSAGE delivery) must NOT carry a spurious msgSerial:0. pointer-nil +
// omitempty keeps them clean.
func TestNonPublishFramesOmitMsgSerial(t *testing.T) {
	frames := map[string]*ProtocolMessage{
		"heartbeat": {Action: ActionHeartbeat},
		"connected": {Action: ActionConnected, ConnectionID: "abc"},
		"message":   {Action: ActionMessage, Channel: new("c"), Messages: []*wire.Message{{Serial: "s:000", Data: wire.MessageStrData("x")}}},
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

// unmarshalMsgpackMap decodes a msgpack blob into a plain map, so a test can
// assert on the keys a frame actually carries.
func unmarshalMsgpackMap(blob []byte, m *map[string]any) error {
	dec := msgpack.NewDecoder(bytes.NewReader(blob))
	return dec.Decode(m)
}

// toInt64 coerces a msgpack-decoded numeric (any int/uint kind) to int64.
func toInt64(v any) (int64, bool) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint()), true
	}
	return 0, false
}
