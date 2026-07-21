package realtime

import (
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
)

// TestResolveModesDefault pins the §4.2/§14.3 rule that a no-mode-bits
// ATTACH resolves to the default set — the message/presence modes plus
// ANNOTATION_PUBLISH (matching the reference's MODE_DEFAULT) — while
// ANNOTATION_SUBSCRIBE stays opt-in.
func TestResolveModesDefault(t *testing.T) {
	got := resolveModes(0)
	if got != defaultModes {
		t.Fatalf("resolveModes(0) = %b, want defaultModes %b", got, defaultModes)
	}
	if got&protocol.FlagAnnotationPublish == 0 {
		t.Errorf("default mode set omits ANNOTATION_PUBLISH: %b", got)
	}
	if got&protocol.FlagAnnotationSubscribe != 0 {
		t.Errorf("default mode set includes opt-in ANNOTATION_SUBSCRIBE: %b", got)
	}
}

// TestResolveModesPassesAnnotationOptIn ensures an ATTACH that explicitly
// requests an annotation mode has it recognised (not masked out).
func TestResolveModesPassesAnnotationOptIn(t *testing.T) {
	req := protocol.FlagSubscribe | protocol.FlagAnnotationSubscribe
	got := resolveModes(req)
	if got != req {
		t.Errorf("resolveModes(SUBSCRIBE|ANNOTATION_SUBSCRIBE) = %b, want %b", got, req)
	}
	pub := protocol.FlagAnnotationPublish
	if resolveModes(pub) != pub {
		t.Errorf("resolveModes(ANNOTATION_PUBLISH) = %b, want %b", resolveModes(pub), pub)
	}
}

// TestParseModesParam covers the comma-separated modes param parsing,
// including unrecognised-token tolerance.
func TestParseModesParam(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"subscribe", protocol.FlagSubscribe},
		{"presence,subscribe", protocol.FlagPresence | protocol.FlagSubscribe},
		{"PRESENCE, Subscribe", protocol.FlagPresence | protocol.FlagSubscribe},
		{"presence_subscribe,publish", protocol.FlagPresenceSubscribe | protocol.FlagPublish},
		{"bogus", 0},
		{"", 0},
	}
	for _, tc := range cases {
		if got := parseModesParam(tc.in); got != tc.want {
			t.Errorf("parseModesParam(%q) = %b, want %b", tc.in, got, tc.want)
		}
	}
}

// TestResolveRequestedModesParamWinsOverFlags pins the §4.2 precedence:
// a modes param overrides the flags mode bits; an empty/invalid param
// falls back to the flags.
func TestResolveRequestedModesParamWinsOverFlags(t *testing.T) {
	// Client sends flags publish|presence_subscribe AND params modes
	// presence,subscribe — the param wins.
	flags := protocol.FlagPublish | protocol.FlagPresenceSubscribe
	got := resolveRequestedModes(flags, map[string]string{"modes": "presence,subscribe"})
	if want := protocol.FlagPresence | protocol.FlagSubscribe; got != want {
		t.Errorf("param modes did not win: got %b, want %b", got, want)
	}
	// No modes param → flags are used.
	if got := resolveRequestedModes(flags, nil); got != flags {
		t.Errorf("resolveRequestedModes(flags, nil) = %b, want %b", got, flags)
	}
	// Invalid modes param → fall back to flags.
	if got := resolveRequestedModes(flags, map[string]string{"modes": "bogus"}); got != flags {
		t.Errorf("invalid modes param not ignored: got %b, want %b", got, flags)
	}
}

// TestModesParamString pins the canonical token order echoed in
// ATTACHED.params.modes.
func TestModesParamString(t *testing.T) {
	if got := modesParamString(protocol.FlagSubscribe | protocol.FlagPresence); got != "presence,subscribe" {
		t.Errorf("modesParamString = %q, want %q", got, "presence,subscribe")
	}
	if got := modesParamString(protocol.FlagPublish | protocol.FlagPresenceSubscribe); got != "publish,presence_subscribe" {
		t.Errorf("modesParamString = %q, want %q", got, "publish,presence_subscribe")
	}
}

// TestAttachedEchoesParamsModes: an ATTACH requesting modes via the params
// map has its ATTACHED echo the effective modes in params.modes (rewritten
// from the flags) and pass other requested params through, while the flags
// carry the same effective mode set the SDK reads channel.modes from
// (RTL4k1/RTL4m).
func TestAttachedEchoesParamsModes(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	c := dialClient(t, srv, "alice")
	drainConnected(t, c)

	sendFrame(t, c, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: new("room"),
		Params:  map[string]string{"modes": "subscribe", "delta": "vcdiff"},
	})
	attached := readFrame(t, c, protocol.FormatJSON, 2*time.Second)
	if attached.Action != protocol.ActionAttached {
		t.Fatalf("frame = %v, want ATTACHED", attached.Action)
	}
	if got := attached.Flags & modeMask; got != protocol.FlagSubscribe {
		t.Errorf("ATTACHED mode flags = %b, want %b", got, protocol.FlagSubscribe)
	}
	if got := attached.Params["modes"]; got != "subscribe" {
		t.Errorf("ATTACHED params.modes = %q, want %q", got, "subscribe")
	}
	if got := attached.Params["delta"]; got != "vcdiff" {
		t.Errorf("ATTACHED params.delta = %q, want %q", got, "vcdiff")
	}
}

// TestPublishNackWithoutPublishMode: a publish on a channel the connection
// is attached to without the publish mode is NACKed 40160; the
// symmetric enforcement to presence.go, so SDKs see the
// expected rejection rather than an ACK.
func TestPublishNackWithoutPublishMode(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	c := dialClient(t, srv, "alice")
	drainConnected(t, c)
	// SUBSCRIBE only — no PUBLISH mode. No self-echo to confuse the read.
	attach(t, c, "room", protocol.FlagSubscribe)

	sendPublish(t, c, "room", 1, "hello")
	nack := readFrame(t, c, protocol.FormatJSON, 2*time.Second)
	if nack.Action != protocol.ActionNack {
		t.Fatalf("frame = %v, want NACK", nack.Action)
	}
	if nack.Error == nil || nack.Error.Code != 40160 {
		t.Errorf("NACK error = %+v, want code 40160", nack.Error)
	}
}
