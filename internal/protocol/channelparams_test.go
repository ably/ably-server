package protocol

import (
	"testing"
)

// TestChannelParamsCoercesNonStringValues pins the lenient decode: an ATTACH
// whose params carry a non-string value (ably-js sends {rewind: 1} as a
// number) must decode without dropping the frame, with the value coerced to
// its string form — in both wire formats.
func TestChannelParamsCoercesNonStringValues(t *testing.T) {
	// Numbers on the wire: JSON emits `1`, msgpack emits an int — both must
	// arrive as the string "1", and a plain string param is untouched.
	jsonFrame := []byte(`{"action":10,"channel":"c","params":{"rewind":1,"foo":"bar","big":1000000}}`)
	var m ProtocolMessage
	if err := Unmarshal(jsonFrame, FormatJSON, &m); err != nil {
		t.Fatalf("JSON Unmarshal (numeric param) failed — frame would be dropped: %v", err)
	}
	if got := m.Params["rewind"]; got != "1" {
		t.Errorf("params[rewind] = %q, want %q", got, "1")
	}
	if got := m.Params["foo"]; got != "bar" {
		t.Errorf("params[foo] = %q, want %q", got, "bar")
	}
	if got := m.Params["big"]; got != "1000000" {
		t.Errorf("params[big] = %q, want %q (no scientific notation)", got, "1000000")
	}

	// Round-trip a msgpack frame carrying a numeric param via a real encoder,
	// exercising the msgpack decode path the way the wire does.
	mp, err := Marshal(&ProtocolMessage{
		Action:  ActionAttach,
		Channel: new("c"),
		Params:  ChannelParams{"rewind": "1"},
	}, FormatMsgpack)
	if err != nil {
		t.Fatalf("Marshal msgpack: %v", err)
	}
	var back ProtocolMessage
	if err := Unmarshal(mp, FormatMsgpack, &back); err != nil {
		t.Fatalf("msgpack Unmarshal: %v", err)
	}
	if got := back.Params["rewind"]; got != "1" {
		t.Errorf("msgpack round-trip params[rewind] = %q, want %q", got, "1")
	}
}
