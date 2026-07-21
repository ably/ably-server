package realtime

import (
	"log/slog"
	"testing"

	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/protocol"
)

// TestAcceptMsgSerialRejectionDoesNotRegressBaseline locks in that a
// rejected (non-monotonic) frame never moves c.lastMsgSerial backward. A
// prior version set c.lastMsgSerial = serial unconditionally before
// deciding accept/reject: a stale retransmit arriving after later serials
// had already been accepted would drag the baseline down, so a SECOND
// retransmit of an already-accepted serial could then be wrongly
// re-accepted against the regressed baseline — corrupting the SDK's
// positional ACK accounting (DESIGN.md §5.2 — a second ACK for an
// already-dequeued msgSerial panics the SDK).
func TestAcceptMsgSerialRejectionDoesNotRegressBaseline(t *testing.T) {
	c := &connection{lastMsgSerial: -1, logger: logging.New(slog.DiscardHandler)}

	steps := []struct {
		serial int64
		want   bool
	}{
		{5, true},  // first publish: adopted as baseline
		{6, true},  // monotonic
		{7, true},  // monotonic
		{8, true},  // monotonic
		{6, false}, // stale retransmit: must not move the baseline off 8
		{7, false}, // a second stale retransmit: must ALSO be rejected —
		// under the old bug, the previous line would have regressed
		// lastMsgSerial to 6, making this one wrongly pass (7 >= 6+1).
	}
	for i, s := range steps {
		got := c.acceptMsgSerial(protocol.ActionMessage, s.serial)
		if got != s.want {
			t.Fatalf("step %d: acceptMsgSerial(%d) = %v, want %v (lastMsgSerial=%d)", i, s.serial, got, s.want, c.lastMsgSerial)
		}
	}
	if c.lastMsgSerial != 8 {
		t.Errorf("final lastMsgSerial = %d, want 8 (unaffected by the rejected retransmits)", c.lastMsgSerial)
	}
}

// TestAcceptMsgSerialForwardSkipAdvancesBaseline confirms a forward skip
// (a gap in msgSerial) is accepted and becomes the new baseline, per the
// documented "a forward skip is allowed" policy.
func TestAcceptMsgSerialForwardSkipAdvancesBaseline(t *testing.T) {
	c := &connection{lastMsgSerial: -1, logger: logging.New(slog.DiscardHandler)}

	if !c.acceptMsgSerial(protocol.ActionMessage, 0) {
		t.Fatal("first publish (serial 0) rejected, want accepted")
	}
	if !c.acceptMsgSerial(protocol.ActionMessage, 5) {
		t.Fatal("forward skip to serial 5 rejected, want accepted")
	}
	if c.lastMsgSerial != 5 {
		t.Errorf("lastMsgSerial = %d, want 5", c.lastMsgSerial)
	}
	if c.acceptMsgSerial(protocol.ActionMessage, 5) {
		t.Error("repeat of serial 5 accepted, want rejected")
	}
	if c.lastMsgSerial != 5 {
		t.Errorf("lastMsgSerial = %d after rejected repeat, want unchanged at 5", c.lastMsgSerial)
	}
}
