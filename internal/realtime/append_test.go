package realtime

import (
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"

	"github.com/gorilla/websocket"
)

// attachWithParams ATTACHes with channel params (e.g. appendMode=full)
// and asserts the ATTACHED response.
func attachWithParams(t *testing.T, ws *websocket.Conn, channel string, flags int64, params map[string]string) {
	t.Helper()
	sendFrame(t, ws, protocol.FormatJSON, &protocol.ProtocolMessage{
		Action:  protocol.ActionAttach,
		Channel: channel,
		Flags:   flags,
		Params:  params,
	})
	if msg := readFrame(t, ws, protocol.FormatJSON, 2*time.Second); msg.Action != protocol.ActionAttached {
		t.Fatalf("expected ATTACHED on %q, got %v", channel, msg.Action)
	}
}

// TestAppendIncrementalToCaughtUpSubscriber: a subscriber that has seen a
// message receives each subsequent append incrementally — a MESSAGE with
// action=append carrying only the delta data and the unchanged serial
// (DESIGN.md §13.3).
func TestAppendIncrementalToCaughtUpSubscriber(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	sub := dialClient(t, srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", protocol.FlagSubscribe)

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPublish|protocol.FlagSubscribe)

	target := publishCreate(t, pub, "room", 1, "Hello")

	create := readMessage(t, sub).Messages[0]
	if create.Action != protocol.MessageCreate || create.Serial != target {
		t.Fatalf("create = %+v, want create with serial %q", create, target)
	}

	sendMutation(t, pub, "room", 2, &protocol.Message{Action: protocol.MessageAppend, Serial: target, Data: ", world"})
	d1 := readMessage(t, sub).Messages[0]
	if d1.Action != protocol.MessageAppend || d1.Data != ", world" || d1.Serial != target {
		t.Errorf("first append = %+v, want incremental append ', world' on %q", d1, target)
	}

	sendMutation(t, pub, "room", 3, &protocol.Message{Action: protocol.MessageAppend, Serial: target, Data: "!"})
	d2 := readMessage(t, sub).Messages[0]
	if d2.Action != protocol.MessageAppend || d2.Data != "!" {
		t.Errorf("second append = %+v, want incremental append '!'", d2)
	}
}

// TestAppendFullOnFirstSightAfterAttach: a subscriber that attaches after
// a message was created and appended to has not seen it, so the first
// append it observes is delivered as a full action=update carrying the
// rolled-up aggregate; later appends then arrive incrementally
// (DESIGN.md §13.3).
func TestAppendFullOnFirstSightAfterAttach(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPublish|protocol.FlagSubscribe)

	target := publishCreate(t, pub, "room", 1, "Hello")
	sendMutation(t, pub, "room", 2, &protocol.Message{Action: protocol.MessageAppend, Serial: target, Data: ", world"})
	// Drain pub's own echo so the append is linked on the live list
	// before the late subscriber computes its attach point.
	readMessage(t, pub)

	sub := dialClient(t, srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", protocol.FlagSubscribe)

	// First append the late subscriber sees: full aggregate (it never saw
	// the create or the earlier append).
	sendMutation(t, pub, "room", 3, &protocol.Message{Action: protocol.MessageAppend, Serial: target, Data: "!"})
	first := readMessage(t, sub).Messages[0]
	if first.Action != protocol.MessageUpdate {
		t.Errorf("first delivery action = %v, want update (full aggregate on first sight)", first.Action)
	}
	if first.Data != "Hello, world!" {
		t.Errorf("first delivery data = %v, want full aggregate %q", first.Data, "Hello, world!")
	}
	if first.Serial != target {
		t.Errorf("first delivery serial = %q, want %q", first.Serial, target)
	}

	// Now caught up: subsequent appends are incremental.
	sendMutation(t, pub, "room", 4, &protocol.Message{Action: protocol.MessageAppend, Serial: target, Data: "?"})
	second := readMessage(t, sub).Messages[0]
	if second.Action != protocol.MessageAppend || second.Data != "?" {
		t.Errorf("second delivery = %+v, want incremental append '?'", second)
	}
}

// TestAppendLastVersionGuarantee: under a run of appends the only
// delivery guarantee is that the last version a subscriber receives is
// the most recent (DESIGN.md §13.3). A caught-up subscriber reconstructs
// the final aggregate by applying each delivery, tolerating a full
// rolled-up update in place of a delta.
func TestAppendLastVersionGuarantee(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	sub := dialClient(t, srv, "")
	drainConnected(t, sub)
	attach(t, sub, "room", protocol.FlagSubscribe)

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPublish|protocol.FlagSubscribe)

	target := publishCreate(t, pub, "room", 1, "0")
	create := readMessage(t, sub).Messages[0]
	if create.Data != "0" {
		t.Fatalf("create data = %v, want 0", create.Data)
	}

	chunks := []string{"1", "2", "3", "4", "5"}
	for i, c := range chunks {
		sendMutation(t, pub, "room", int64(i+2), &protocol.Message{Action: protocol.MessageAppend, Serial: target, Data: c})
	}

	got := "0"
	for range chunks {
		m := readMessage(t, sub).Messages[0]
		switch m.Action {
		case protocol.MessageAppend:
			got += m.Data.(string)
		case protocol.MessageUpdate:
			// A conflated full rolled-up update replaces the accumulator.
			got = m.Data.(string)
		default:
			t.Fatalf("unexpected action %v during append run", m.Action)
		}
	}
	if got != "012345" {
		t.Errorf("reconstructed = %q, want the most recent aggregate %q", got, "012345")
	}
}

// TestAppendModeFullDeliversFullVersions: a subscriber that attaches with
// appendMode=full always receives full rolled-up action=update versions
// rather than incremental append deltas (DESIGN.md §13.3).
func TestAppendModeFullDeliversFullVersions(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	sub := dialClient(t, srv, "")
	drainConnected(t, sub)
	attachWithParams(t, sub, "room", protocol.FlagSubscribe, map[string]string{ParamAppendMode: AppendModeFull})

	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPublish|protocol.FlagSubscribe)

	target := publishCreate(t, pub, "room", 1, "Hello")
	if create := readMessage(t, sub).Messages[0]; create.Action != protocol.MessageCreate {
		t.Fatalf("create action = %v, want create", create.Action)
	}

	sendMutation(t, pub, "room", 2, &protocol.Message{Action: protocol.MessageAppend, Serial: target, Data: ", world"})
	m1 := readMessage(t, sub).Messages[0]
	if m1.Action != protocol.MessageUpdate || m1.Data != "Hello, world" {
		t.Errorf("append under appendMode=full = %+v, want full update %q", m1, "Hello, world")
	}

	sendMutation(t, pub, "room", 3, &protocol.Message{Action: protocol.MessageAppend, Serial: target, Data: "!"})
	m2 := readMessage(t, sub).Messages[0]
	if m2.Action != protocol.MessageUpdate || m2.Data != "Hello, world!" {
		t.Errorf("second append under appendMode=full = %+v, want full update %q", m2, "Hello, world!")
	}
}

// TestAppendIncompatibleDataNacked: an append whose data type cannot
// concatenate onto the target's current data is NACKed (DESIGN.md §13.3).
func TestAppendIncompatibleDataNacked(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	pub := dialClient(t, srv, "alice")
	drainConnected(t, pub)
	attach(t, pub, "room", protocol.FlagPublish|protocol.FlagSubscribe)

	target := publishCreate(t, pub, "room", 1, "text")

	// Append a numeric value onto a string message: concatenation is
	// defined only string-onto-string / binary-onto-binary.
	sendMutation(t, pub, "room", 2, &protocol.Message{Action: protocol.MessageAppend, Serial: target, Data: 42})
	for {
		f := readFrame(t, pub, protocol.FormatJSON, 2*time.Second)
		if f.Action == protocol.ActionMessage {
			continue // skip the create echo
		}
		if f.Action != protocol.ActionNack || f.PublishSerial() != 2 {
			t.Fatalf("frame = %+v, want NACK on msgSerial 2", f)
		}
		break
	}
}
