package handles

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ably/server-protocol/go/channel"
	"github.com/ably/server-protocol/go/wire"
)

// TestOccupancy_ModeChangeIsReportedImmediately checks the half of the
// reporting policy that does not wait: a change that makes the channel
// occupied in a mode it was not — the first subscriber, the first publisher —
// reaches the aggregate without waiting for the roll-up.
//
// The roll-up is set far longer than the test's patience, so a pass can only
// mean the change bypassed it.
func TestOccupancy_ModeChangeIsReportedImmediately(t *testing.T) {
	withReportInterval(t, time.Hour)
	held := newTestChannel(t, "occupancy-immediate")
	live := held.live()

	live.AddOccupancy(channel.Mode(channel.MESSAGE_SUBSCRIBE))

	awaitOccupancy(t, live, time.Second, "the first subscriber", func(agg *wire.ChannelOccupancy) bool {
		return agg.GetConnections() == 1 && agg.GetSubscribers() == 1
	})

	// A publisher arriving is a mode change too — the channel was not occupied
	// by a publisher before — so it is immediate for the same reason.
	live.AddOccupancy(channel.Mode(channel.MESSAGE_PUBLISH))

	awaitOccupancy(t, live, time.Second, "the first publisher", func(agg *wire.ChannelOccupancy) bool {
		return agg.GetConnections() == 2 && agg.GetPublishers() == 1
	})
}

// TestOccupancy_NonModeChangeIsRolledUp checks the other half: a second holder
// of a mode the channel already has changes only the numbers, and waits for
// the roll-up rather than being reported as it happens.
//
// This is what keeps a busy channel from waking every occupancy subscriber on
// every attach. The test asserts both halves of that — that the change is not
// reported early, and that it is not dropped.
func TestOccupancy_NonModeChangeIsRolledUp(t *testing.T) {
	withReportInterval(t, 300*time.Millisecond)
	held := newTestChannel(t, "occupancy-rollup")
	live := held.live()

	// The first subscriber is a mode change, so it lands at once and gives the
	// test a settled aggregate to measure the second against.
	live.AddOccupancy(channel.Mode(channel.MESSAGE_SUBSCRIBE))
	awaitOccupancy(t, live, time.Second, "the first subscriber", func(agg *wire.ChannelOccupancy) bool {
		return agg.GetConnections() == 1
	})

	live.AddOccupancy(channel.Mode(channel.MESSAGE_SUBSCRIBE))

	// Not reported as it happens: the channel was already occupied by a
	// subscriber, so nothing about its shape changed.
	time.Sleep(50 * time.Millisecond)
	if got := live.GetAggregateOccupancy().GetConnections(); got != 1 {
		t.Errorf("connections = %d immediately after a second subscriber, want the rolled-up 1", got)
	}

	// And not dropped: the next tick carries it.
	awaitOccupancy(t, live, 2*time.Second, "the roll-up to carry the second subscriber", func(agg *wire.ChannelOccupancy) bool {
		return agg.GetConnections() == 2 && agg.GetSubscribers() == 2
	})
}

// TestOccupancy_LastHolderLeavingIsReportedImmediately checks the mirror of
// the first case: the channel emptying is a mode change too, so a client
// watching occupancy is told the channel is empty at once rather than being
// left to believe it is still occupied for another interval.
func TestOccupancy_LastHolderLeavingIsReportedImmediately(t *testing.T) {
	withReportInterval(t, time.Hour)
	held := newTestChannel(t, "occupancy-empty")
	live := held.live()

	live.AddOccupancy(channel.Mode(channel.MESSAGE_SUBSCRIBE))
	awaitOccupancy(t, live, time.Second, "the subscriber", func(agg *wire.ChannelOccupancy) bool {
		return agg.GetConnections() == 1
	})

	live.RemoveOccupancy(channel.Mode(channel.MESSAGE_SUBSCRIBE))

	awaitOccupancy(t, live, time.Second, "the channel to empty", func(agg *wire.ChannelOccupancy) bool {
		return agg.GetConnections() == 0 && agg.GetSubscribers() == 0
	})
}

// TestOccupancy_InbandSubscriberIsToldTheAggregate checks what a client
// watching `[meta]occupancy` is actually served: the live value carries a
// message for the category it asked for, and it is updated as the aggregate
// moves.
//
// It also checks the value a subscriber gets on arrival. A channel's occupancy
// may not change for hours, so an attachment told nothing until the next
// change would report nothing at all.
func TestOccupancy_InbandSubscriberIsToldTheAggregate(t *testing.T) {
	withReportInterval(t, time.Hour)
	held := newTestChannel(t, "occupancy-inband")
	live := held.live()

	live.AddOccupancy(channel.Mode(channel.MESSAGE_SUBSCRIBE))
	awaitOccupancy(t, live, time.Second, "the subscriber", func(agg *wire.ChannelOccupancy) bool {
		return agg.GetConnections() == 1
	})

	// Subscribing after the fact: the seed carries the occupancy as it stands.
	value := live.InbandOccupancy(channel.OccupancyAll)
	if value == nil {
		t.Fatal("InbandOccupancy answered with no value to watch")
	}
	if got := inbandConnections(t, value.Get()); got != 1 {
		t.Errorf("a subscriber arriving was seeded with connections = %v, want 1", got)
	}

	// And it is updated as the aggregate moves.
	state := value.State()
	live.AddOccupancy(channel.Mode(channel.MESSAGE_PUBLISH))

	select {
	case <-state.Notify:
		if got := inbandConnections(t, state.Next.Value); got != 2 {
			t.Errorf("the inband value reported connections = %v, want 2", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the inband occupancy value was never updated")
	}
}

// TestOccupancy_OnlyTheRequestedCategoryIsReported checks that a client asking
// for one metric is sent that one: the category is what the value is rendered
// for, not a filter the client applies.
func TestOccupancy_OnlyTheRequestedCategoryIsReported(t *testing.T) {
	withReportInterval(t, time.Hour)
	held := newTestChannel(t, "occupancy-category")
	live := held.live()

	live.AddOccupancy(channel.Mode(channel.MESSAGE_SUBSCRIBE))
	awaitOccupancy(t, live, time.Second, "the subscriber", func(agg *wire.ChannelOccupancy) bool {
		return agg.GetConnections() == 1
	})

	metrics := inbandMetrics(t, live.InbandOccupancy(channel.OccupancySubscribers).Get())
	if _, ok := metrics["subscribers"]; !ok {
		t.Errorf("a subscriber-category watcher was not sent subscribers: %v", metrics)
	}
	if _, ok := metrics["connections"]; ok {
		t.Errorf("a subscriber-category watcher was sent connections too: %v", metrics)
	}
}

// withReportInterval shrinks (or stretches) the roll-up for one test and puts
// it back afterwards.
//
// Stretching it past the test's own patience is how a test asserts that
// something did NOT wait for it: with the roll-up an hour away, anything that
// arrives arrived by the immediate path.
func withReportInterval(t *testing.T, d time.Duration) {
	t.Helper()
	previous := reportInterval
	reportInterval = d
	t.Cleanup(func() { reportInterval = previous })
}

// awaitOccupancy polls the aggregate until it satisfies want. A nil aggregate
// is one that has not settled yet, not an answer.
func awaitOccupancy(t *testing.T, c *liveChannel, within time.Duration, waitingFor string, want func(*wire.ChannelOccupancy) bool) {
	t.Helper()

	deadline := time.Now().Add(within)
	var last *wire.ChannelOccupancy
	for time.Now().Before(deadline) {
		last = c.GetAggregateOccupancy()
		if last != nil && want(last) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("waiting for %s; the aggregate settled at %+v", waitingFor, last)
}

// inbandConnections is the connections metric one inband occupancy message
// carries.
func inbandConnections(t *testing.T, msg *channel.CachedEncodingChannelMessage) float64 {
	t.Helper()
	metrics := inbandMetrics(t, msg)
	connections, ok := metrics["connections"]
	if !ok {
		t.Fatalf("the inband message carries no connections metric: %v", metrics)
	}
	return connections
}

// inbandMetrics decodes the metrics an inband occupancy message carries, which
// ride in the message data as JSON: {"metrics":{"connections":1,...}}.
func inbandMetrics(t *testing.T, msg *channel.CachedEncodingChannelMessage) map[string]float64 {
	t.Helper()
	if msg == nil || msg.ChannelMessage == nil || len(msg.GetMessages()) == 0 {
		t.Fatal("the inband occupancy value carries no message")
	}
	data := msg.GetMessages()[0].Data
	if data == nil {
		t.Fatal("the inband occupancy message carries no data")
	}

	var payload struct {
		Metrics map[string]float64 `json:"metrics"`
	}
	if err := json.Unmarshal([]byte(data.AsStr()), &payload); err != nil {
		t.Fatalf("decoding the inband occupancy metrics (%s): %s", data.AsStr(), err)
	}
	return payload.Metrics
}

// TestServeWebSocket_InbandOccupancy is the occupancy path end to end over a
// real connection: a client attaches asking for occupancy, is sent the
// channel's metrics as a meta message, and is sent them again when a second
// client attaching changes the shape of the channel.
//
// The roll-up is stretched past the test's patience, so what it observes is
// the immediate path — which is the one that matters for a client watching a
// channel come alive.
func TestServeWebSocket_InbandOccupancy(t *testing.T) {
	withReportInterval(t, time.Hour)
	stack := newTestServers(t)

	watcher := dial(t, stack)
	defer watcher.Close()
	readFrame(t, watcher) // CONNECTED

	if err := watcher.WriteJSON(map[string]any{
		"action": actionAttach, "channel": "occupancy-e2e",
		"params": map[string]any{"occupancy": "metrics"},
	}); err != nil {
		t.Fatalf("ATTACH: %s", err)
	}
	if attached := nextFrameOfAction(t, watcher, actionAttached); attached == nil {
		t.Fatal("the watcher did not attach")
	}

	// The occupancy it is sent counts itself: it is attached, so the channel
	// has one connection.
	awaitOccupancyMetrics(t, watcher, "the watcher to be told it occupies the channel",
		func(m map[string]float64) bool { return m["connections"] == 1 })

	// A second client attaching for objects is a mode change: the watcher's
	// own default modes cover publishing, subscribing and presence, but not
	// objects, so the channel becomes occupied in a mode it was not. The
	// watcher hears about it without waiting for the roll-up.
	//
	// A second *publisher* would not do — the watcher is already one by
	// default — and that is the rule working rather than a gap in it: only
	// the counts would move, and moving counts are what the roll-up is for.
	objectClient := dial(t, stack)
	defer objectClient.Close()
	readFrame(t, objectClient) // CONNECTED
	if err := objectClient.WriteJSON(map[string]any{
		"action": actionAttach, "channel": "occupancy-e2e", "flags": 1 << 24,
	}); err != nil {
		t.Fatalf("object client ATTACH: %s", err)
	}
	if attached := nextFrameOfAction(t, objectClient, actionAttached); attached == nil {
		t.Fatal("the object client did not attach")
	}

	awaitOccupancyMetrics(t, watcher, "the watcher to be told about the object subscriber",
		func(m map[string]float64) bool {
			return m["connections"] == 2 && m["objectSubscribers"] == 1
		})
}

// TestServeWebSocket_InbandOccupancyRollsUp is the other path over the wire: a
// change that alters no mode is still delivered, after the roll-up rather than
// on the spot.
//
// It is the companion to the test above, and it exists because getting this
// wrong looks exactly like getting it right. A rolled-up change that was
// silently dropped and one that is merely late are indistinguishable until
// something waits past the interval — so something does.
func TestServeWebSocket_InbandOccupancyRollsUp(t *testing.T) {
	withReportInterval(t, 300*time.Millisecond)
	stack := newTestServers(t)

	watcher := dial(t, stack)
	defer watcher.Close()
	readFrame(t, watcher) // CONNECTED

	if err := watcher.WriteJSON(map[string]any{
		"action": actionAttach, "channel": "occupancy-rollup-e2e",
		"params": map[string]any{"occupancy": "metrics"},
	}); err != nil {
		t.Fatalf("ATTACH: %s", err)
	}
	if attached := nextFrameOfAction(t, watcher, actionAttached); attached == nil {
		t.Fatal("the watcher did not attach")
	}
	awaitOccupancyMetrics(t, watcher, "the watcher to be told it occupies the channel",
		func(m map[string]float64) bool { return m["connections"] == 1 })

	// A second publisher: the watcher publishes by default, so the channel was
	// already occupied by one and only the count moves.
	publisher := dial(t, stack)
	defer publisher.Close()
	readFrame(t, publisher) // CONNECTED
	if err := publisher.WriteJSON(map[string]any{
		"action": actionAttach, "channel": "occupancy-rollup-e2e", "flags": 1 << 17,
	}); err != nil {
		t.Fatalf("publisher ATTACH: %s", err)
	}
	if attached := nextFrameOfAction(t, publisher, actionAttached); attached == nil {
		t.Fatal("the publisher did not attach")
	}

	start := time.Now()
	awaitOccupancyMetrics(t, watcher, "the roll-up to carry the second publisher",
		func(m map[string]float64) bool {
			return m["connections"] == 2 && m["publishers"] == 2
		})
	// Late, not immediate: it waited for a tick rather than bypassing it.
	if waited := time.Since(start); waited < 100*time.Millisecond {
		t.Errorf("the change arrived after %s, want it to have waited for the roll-up", waited)
	}
}

// awaitOccupancyMetrics reads inband occupancy messages until one satisfies
// want.
//
// It waits rather than asserting on the first message because a client is
// promised the occupancy, not that the first thing it is told is already
// final: an aggregate assembled across nodes settles, and a watcher sees it
// settle. What is asserted here is where it settles, and — where a test cares
// — how long that took.
func awaitOccupancyMetrics(t *testing.T, client *websocket.Conn, waitingFor string, want func(map[string]float64) bool) {
	t.Helper()

	var last map[string]float64
	for range 10 {
		last = nextOccupancyMetrics(t, client)
		if want(last) {
			return
		}
	}
	t.Fatalf("waiting for %s; the last occupancy reported was %v", waitingFor, last)
}

// nextOccupancyMetrics reads frames until an inband occupancy meta message
// arrives and returns the metrics it carries.
func nextOccupancyMetrics(t *testing.T, client *websocket.Conn) map[string]float64 {
	t.Helper()

	for range 10 {
		msg := readFrame(t, client)
		messages, _ := msg["messages"].([]any)
		if len(messages) == 0 {
			continue
		}
		first, _ := messages[0].(map[string]any)
		if name, _ := first["name"].(string); name != "[meta]occupancy" {
			continue
		}
		var payload struct {
			Metrics map[string]float64 `json:"metrics"`
		}
		data, _ := first["data"].(string)
		if err := json.Unmarshal([]byte(data), &payload); err != nil {
			t.Fatalf("decoding the inband occupancy data (%s): %s", data, err)
		}
		return payload.Metrics
	}
	t.Fatal("no inband occupancy message arrived")
	return nil
}
