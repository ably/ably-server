package protocol_test

import (
	"testing"

	"github.com/ably/ably-server/internal/protocol"
)

func TestPresenceActionString(t *testing.T) {
	cases := map[protocol.PresenceAction]string{
		protocol.PresenceAbsent:     "absent",
		protocol.PresencePresent:    "present",
		protocol.PresenceEnter:      "enter",
		protocol.PresenceLeave:      "leave",
		protocol.PresenceUpdate:     "update",
		protocol.PresenceAction(99): "unknown",
	}
	for action, want := range cases {
		if got := action.String(); got != want {
			t.Errorf("PresenceAction(%d).String() = %q, want %q", action, got, want)
		}
	}
}

// TestPresenceFrameRoundTrips encodes a PRESENCE ProtocolMessage (whose
// presence-bearing ChannelMessage carries a PresenceMessage) through
// both wire formats and asserts the decoded value matches.
func TestPresenceFrameRoundTrips(t *testing.T) {
	for _, f := range []protocol.Format{protocol.FormatJSON, protocol.FormatMsgpack} {
		t.Run(f.String(), func(t *testing.T) {
			in := &protocol.ProtocolMessage{
				Action:  protocol.ActionPresence,
				Channel: new("room"),
				Flags:   protocol.FlagPresenceSubscribe,
				Presence: []*protocol.PresenceMessage{
					{
						ID:           "client-key",
						Serial:       "00000000000123-000@abcdef0123:000",
						Action:       protocol.PresenceEnter,
						ClientID:     "alice",
						ConnectionID: "conn-1",
						Data:         "hello",
						Timestamp:    1700000000000,
					},
				},
			}

			blob, err := protocol.Marshal(in, f)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var out protocol.ProtocolMessage
			if err := protocol.Unmarshal(blob, f, &out); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}

			if out.Action != protocol.ActionPresence {
				t.Errorf("Action = %v, want presence", out.Action)
			}
			if out.Flags != protocol.FlagPresenceSubscribe {
				t.Errorf("Flags = %d, want %d", out.Flags, protocol.FlagPresenceSubscribe)
			}
			if len(out.Presence) != 1 {
				t.Fatalf("Presence len = %d, want 1", len(out.Presence))
			}
			got := out.Presence[0]
			in0 := in.Presence[0]
			if got.ID != in0.ID || got.Serial != in0.Serial || got.Action != in0.Action ||
				got.ClientID != in0.ClientID || got.ConnectionID != in0.ConnectionID ||
				got.Timestamp != in0.Timestamp {
				t.Errorf("PresenceMessage round-trip mismatch:\n got = %+v\nwant = %+v", got, in0)
			}
			if got.Data != in0.Data {
				t.Errorf("Data = %v, want %v", got.Data, in0.Data)
			}
		})
	}
}

// TestPresenceChannelMessageRoundTrips covers the storage-side unit: a
// ChannelMessage carrying Presence (rather than Messages).
func TestPresenceChannelMessageRoundTrips(t *testing.T) {
	in := &protocol.ProtocolMessage{
		Action:   protocol.ActionSync,
		Channel:  new("room"),
		Messages: nil,
		Presence: []*protocol.PresenceMessage{
			{Action: protocol.PresencePresent, ClientID: "bob", ConnectionID: "conn-2"},
			{Action: protocol.PresencePresent, ClientID: "carol", ConnectionID: "conn-3"},
		},
	}
	blob, err := protocol.Marshal(in, protocol.FormatJSON)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var out protocol.ProtocolMessage
	if err := protocol.Unmarshal(blob, protocol.FormatJSON, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(out.Presence) != 2 {
		t.Fatalf("Presence len = %d, want 2", len(out.Presence))
	}
	if len(out.Messages) != 0 {
		t.Errorf("Messages len = %d, want 0 (presence frame carries no messages)", len(out.Messages))
	}
}
