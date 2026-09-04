package handles

import (
	"testing"
	"time"

	"github.com/ably/server-protocol/go/channel"
	protoproto "github.com/ably/server-protocol/go/protocol"
	"github.com/ably/server-protocol/go/wire"
)

// TestGetPresence_PagesInCursorOrder checks the presence sync this server
// serves through the shared protocol code: the members come out in cursor
// order, split into pages of the requested size, and each page but the last
// carries the cursor that resumes at the member which did not fit.
//
// The framing is the shared code's, so what this proves is that this server
// feeds it correctly — the ordering and the conversion.
func TestGetPresence_PagesInCursorOrder(t *testing.T) {
	ctx := t.Context()
	ch := newChannelWithMembers(t, "presence-test",
		presenceEnter("conn-b", "alice"),
		presenceEnter("conn-a", "carol"),
		presenceEnter("conn-a", "bob"),
	)

	msgs, errInfo := ch.presence().GetPresence(ctx, channel.PresenceParams{SyncID: "sync1", Limit: 2})
	if errInfo != nil {
		t.Fatalf("GetPresence: %s", errInfo)
	}

	if len(msgs) != 2 {
		t.Fatalf("got %d pages, want 2", len(msgs))
	}

	// conn-a sorts before conn-b, and bob before carol within conn-a.
	got := make([]string, 0, 3)
	for _, msg := range msgs {
		for _, m := range msg.Presence {
			got = append(got, m.ConnectionId+":"+m.GetClientId())
		}
	}
	want := []string{"conn-a:bob", "conn-a:carol", "conn-b:alice"}
	if len(got) != len(want) {
		t.Fatalf("got %v members, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("member %d = %q, want %q", i, got[i], want[i])
		}
	}

	// The first page is full, so it carries the cursor for the third member;
	// the last page has none.
	if msgs[0].ChannelSerial != "sync1:conn-b:alice" {
		t.Errorf("first page cursor = %q, want %q", msgs[0].ChannelSerial, "sync1:conn-b:alice")
	}
	if msgs[1].ChannelSerial != "sync1:" {
		t.Errorf("last page cursor = %q, want %q", msgs[1].ChannelSerial, "sync1:")
	}
}

// TestGetPresence_EmptyChannelStillSyncs checks that a client attaching to a
// channel nobody is present on is still told the sync is complete, rather than
// being sent nothing.
func TestGetPresence_EmptyChannelStillSyncs(t *testing.T) {
	ch := newChannelWithMembers(t, "empty-test")

	msgs, errInfo := ch.presence().GetPresence(t.Context(), channel.PresenceParams{SyncID: "sync1"})
	if errInfo != nil {
		t.Fatalf("GetPresence: %s", errInfo)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d pages, want 1", len(msgs))
	}
	if len(msgs[0].Presence) != 0 {
		t.Errorf("got %d members on an empty channel", len(msgs[0].Presence))
	}
}

// TestGetPresence_OneClientID checks that a client asking after a single
// clientId is told about that one and no others.
func TestGetPresence_OneClientID(t *testing.T) {
	ch := newChannelWithMembers(t, "filter-test",
		presenceEnter("conn-a", "alice"),
		presenceEnter("conn-a", "bob"),
	)

	msgs, errInfo := ch.presence().GetPresence(t.Context(), channel.PresenceParams{SyncID: "s", ClientId: "bob"})
	if errInfo != nil {
		t.Fatalf("GetPresence: %s", errInfo)
	}

	var got []string
	for _, msg := range msgs {
		for _, m := range msg.Presence {
			got = append(got, m.GetClientId())
		}
	}
	if len(got) != 1 || got[0] != "bob" {
		t.Errorf("got %v, want [bob]", got)
	}
}

// newChannelWithMembers is a channel already holding the given members when
// its presence map first reads the set, which is what a client attaching to a
// channel someone is already present on sees.
func newChannelWithMembers(t *testing.T, name string, members ...*wire.PresenceMessage) heldChannel {
	t.Helper()

	m, store := newTestChannels(t)
	stored, err := store.GetChannel(t.Context(), name)
	if err != nil {
		t.Fatalf("GetChannel: %s", err)
	}
	if len(members) > 0 {
		if _, _, err := stored.PublishPresence(t.Context(), members); err != nil {
			t.Fatalf("PublishPresence: %s", err)
		}
	}
	return getTestChannel(t, m, store, name)
}

func presenceEnter(connID, clientID string) *wire.PresenceMessage {
	return &wire.PresenceMessage{
		Action:       wire.PresenceMessage_ENTER,
		ConnectionId: connID,
		ClientId:     new(clientID),
		Data:         wire.MessageStrData("hello"),
	}
}

// TestGetPresence_LiveChangesReachASyncedSet publishes a leave after the
// presence set has been synced, and checks the next sync reflects it.
//
// The map reads the set once and serves every later sync from what it holds,
// so a change that arrives afterwards only reaches a client if the live
// messages are folded into it. Doing that folding — deciding which of two
// messages for a member wins, and what a leave means while a sync is in
// flight — is the map's, and is why it is shared code rather than a re-read.
func TestGetPresence_LiveChangesReachASyncedSet(t *testing.T) {
	ctx := t.Context()
	ch := newChannelWithMembers(t, "presence-live",
		presenceEnter("conn-a", "alice"),
		presenceEnter("conn-a", "bob"),
	)

	// The first sync is what reads the set.
	msgs, errInfo := ch.presence().GetPresence(ctx, channel.PresenceParams{SyncID: "s"})
	if errInfo != nil {
		t.Fatalf("GetPresence: %s", errInfo)
	}
	if got := countMembers(msgs); got != 2 {
		t.Fatalf("first sync had %d members, want 2", got)
	}

	if _, _, err := ch.stored.PublishPresence(ctx, []*wire.PresenceMessage{{
		Action:       wire.PresenceMessage_LEAVE,
		ConnectionId: "conn-a",
		ClientId:     new("bob"),
	}}); err != nil {
		t.Fatalf("PublishPresence: %s", err)
	}

	// The leave reaches the map over the channel's own tail, so wait for it.
	deadline := time.Now().Add(2 * time.Second)
	for {
		msgs, errInfo = ch.presence().GetPresence(ctx, channel.PresenceParams{SyncID: "s"})
		if errInfo != nil {
			t.Fatalf("GetPresence after the leave: %s", errInfo)
		}
		if countMembers(msgs) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("bob was still present %v after leaving", 2*time.Second)
		}
		time.Sleep(5 * time.Millisecond)
	}

	for _, msg := range msgs {
		for _, m := range msg.Presence {
			if m.GetClientId() != "alice" {
				t.Errorf("got %q present, want only alice", m.GetClientId())
			}
		}
	}
}

func countMembers(msgs []*protoproto.ProtocolMessage) int {
	n := 0
	for _, msg := range msgs {
		n += len(msg.Presence)
	}
	return n
}
