package handles

import (
	"context"
	"testing"
	"time"

	"github.com/ably/server-protocol/go/channel"
	"github.com/ably/server-protocol/go/logging/testlog"
	"github.com/ably/server-protocol/go/wire"
)

// connectionLeave is the message a dropped connection produces: one LEAVE
// naming the connection and no client, because it is the whole connection that
// has gone.
func connectionLeave(connID string) *wire.ChannelMessage {
	return &wire.ChannelMessage{
		Action: wire.ProtocolMessageAction_ACTION_PRESENCE,
		Presence: []*wire.PresenceMessage{{
			Action:       wire.PresenceMessage_LEAVE,
			ConnectionId: connID,
		}},
	}
}

// TestConnectionLeaveRemovesEveryMemberOfThatConnection checks that the leave a
// dropped connection produces takes that connection's members with it.
//
// This server keys its presence set by connection and client together, so a
// leave naming no client matched nobody and left every one of that connection's
// members present.
func TestConnectionLeaveRemovesEveryMemberOfThatConnection(t *testing.T) {
	ctx := t.Context()
	lc := newChannelWithMembers(t, "conn-leave",
		presenceEnter("conn-a", "alice"),
		presenceEnter("conn-a", "bob"),
		presenceEnter("conn-b", "carol"),
	)

	msgs, errInfo := lc.presence().GetPresence(ctx, channel.PresenceParams{SyncID: "s"})
	if errInfo != nil {
		t.Fatalf("GetPresence: %s", errInfo)
	}
	if got := countMembers(msgs); got != 3 {
		t.Fatalf("started with %d members, want 3", got)
	}

	if err := lc.live().Publish(ctx, connectionLeave("conn-a"), nil); err != nil {
		t.Fatalf("publishing the connection leave: %s", err)
	}

	// The leave reaches the presence map over the channel's own tail, so wait
	// for it rather than reading the set the instant the publish returns.
	var left []string
	deadline := time.Now().Add(2 * time.Second)
	for {
		msgs, errInfo = lc.presence().GetPresence(ctx, channel.PresenceParams{SyncID: "s"})
		if errInfo != nil {
			t.Fatalf("GetPresence after the leave: %s", errInfo)
		}
		left = left[:0]
		for _, msg := range msgs {
			for _, m := range msg.Presence {
				left = append(left, m.ConnectionId+":"+m.GetClientId())
			}
		}
		if len(left) == 1 && left[0] == "conn-b:carol" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after conn-a left, present = %v, want [conn-b:carol]", left)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestDelayedLeaveIsPublishedWhenTheConnectionStaysAway is the half that must
// still happen: a member whose connection is gone for good has to leave, or the
// channel keeps them present forever.
func TestDelayedLeaveIsPublishedWhenTheConnectionStaysAway(t *testing.T) {
	published := make(chan *wire.ChannelMessage, 1)
	leaves := newDelayedLeaves(t.Context(), func(_ context.Context, leave *wire.ChannelMessage) {
		published <- leave
	}, testlog.Logger(t))

	leaves.hold(connectionLeave("conn-a"), "conn-a", time.Millisecond, nil)

	select {
	case leave := <-published:
		if got := leave.Presence[0].ConnectionId; got != "conn-a" {
			t.Errorf("published a leave for %q, want conn-a", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a held leave was never published, so the member stays present forever")
	}
}

// TestDelayedLeaveIsDroppedWhenTheConnectionReturns is the other half, and the
// reason for holding it at all: a client that reconnects inside its window
// never appears to its fellow members to have left.
func TestDelayedLeaveIsDroppedWhenTheConnectionReturns(t *testing.T) {
	published := make(chan *wire.ChannelMessage, 1)
	leaves := newDelayedLeaves(t.Context(), func(_ context.Context, leave *wire.ChannelMessage) {
		published <- leave
	}, testlog.Logger(t))

	leaves.hold(connectionLeave("conn-a"), "conn-a", 50*time.Millisecond, nil)
	leaves.cancel("conn-a")

	select {
	case leave := <-published:
		t.Errorf("a returning connection's leave was published anyway: %v", leave)
	case <-time.After(250 * time.Millisecond):
	}
}
