package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/integration"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
	"github.com/ably/server-protocol/go/wire"
)

// TestCrashedNodePresenceReaped enters a presence member on node A,
// kills node A without teardown (so it stops bumping its lease), and
// verifies node B reaps the orphan within a lease window: the member
// disappears from Members and a synthetic LEAVE reaches node B's
// appender exactly once.
func TestCrashedNodePresenceReaped(t *testing.T) {
	integration.Require(t)
	// Shrink the lease/bump/reaper cadences so the test runs in seconds.
	defer swapPresenceTimings(1*time.Second, 200*time.Millisecond, 200*time.Millisecond)()

	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	// Node A: the node that will "crash". Enters presence; no appender.
	sa, err := Open(ctx, Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open node A: %v", err)
	}
	// Close (idempotent) is deferred so it also runs on a failure path;
	// registered after the timing-restore defer so both nodes' goroutines
	// stop before the shared timing vars are restored (else the restore
	// write races their reads).
	defer func() { _ = sa.Close() }()
	chA, err := sa.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel node A: %v", err)
	}

	// Node B: the surviving node. Observes presence via its appender.
	sb, err := Open(ctx, Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open node B: %v", err)
	}
	defer func() { _ = sb.Close() }()
	recB := &presenceRecorder{}
	chB, err := sb.Channel(ctx, "room", recB)
	if err != nil {
		t.Fatalf("Channel node B: %v", err)
	}

	// Member enters on node A.
	if _, _, err := chA.StorePresence(ctx, []*wire.PresenceMessage{{
		Action:       wire.PresenceMessage_ENTER,
		ClientId:     new("alice"),
		ConnectionId: "connA",
	}}); err != nil {
		t.Fatalf("enter presence: %v", err)
	}

	// Node B must first observe alice as a live member (the ENTER folded
	// into the table and its NOTIFY reached B).
	waitFor(t, 10*time.Second, "node B to see alice enter", func() bool {
		return memberPresent(t, ctx, chB, "alice")
	})

	// Crash node A: Close stops its lease-bump loop, so alice's row lapses.
	if err := sa.Close(); err != nil {
		t.Fatalf("Close node A: %v", err)
	}

	// Within a lease window, node B's reaper deletes the orphan and emits
	// a synthetic LEAVE.
	waitFor(t, 10*time.Second, "alice to be reaped from Members", func() bool {
		return !memberPresent(t, ctx, chB, "alice")
	})
	waitFor(t, 10*time.Second, "synthetic LEAVE to reach node B", func() bool {
		return recB.leaveCount("alice") >= 1
	})

	// The LEAVE must be emitted exactly once (single reap, single node).
	// Give any duplicate reap a chance to (wrongly) fire, then assert.
	time.Sleep(2 * presenceReaperInterval)
	if got := recB.leaveCount("alice"); got != 1 {
		t.Fatalf("node B saw %d synthetic LEAVEs for alice, want exactly 1", got)
	}
}

// TestStaticFixturePresenceSurvivesReaper enters a static fixture member
// (storage.WithStaticPresence) on node A, then closes node A so its
// lease-bump loop stops. A normal member would lapse and be reaped, but a
// fixture member carries an 'infinity' lease and a sentinel owner, so
// node B's reaper never removes it.
func TestStaticFixturePresenceSurvivesReaper(t *testing.T) {
	integration.Require(t)
	defer swapPresenceTimings(1*time.Second, 200*time.Millisecond, 200*time.Millisecond)()

	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	sa, err := Open(ctx, Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open node A: %v", err)
	}
	defer func() { _ = sa.Close() }()
	chA, err := sa.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel node A: %v", err)
	}

	sb, err := Open(ctx, Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open node B: %v", err)
	}
	defer func() { _ = sb.Close() }()
	chB, err := sb.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel node B: %v", err)
	}

	// Seed a static fixture member on node A.
	if _, _, err := chA.StorePresence(storage.WithStaticPresence(ctx), []*wire.PresenceMessage{{
		Action:       wire.PresenceMessage_ENTER,
		ClientId:     new("fixture_client"),
		ConnectionId: "connFixture",
		Data:         wire.MessageStrData("true"),
	}}); err != nil {
		t.Fatalf("seed fixture presence: %v", err)
	}

	waitFor(t, 10*time.Second, "node B to see the fixture member", func() bool {
		return memberPresent(t, ctx, chB, "fixture_client")
	})

	// Node A "crashes": its bump loop stops. A normal member would lapse
	// within a lease window; wait several reaper cycles and assert the
	// fixture member is still present on node B.
	if err := sa.Close(); err != nil {
		t.Fatalf("Close node A: %v", err)
	}
	time.Sleep(presenceLeaseWindow + 10*presenceReaperInterval)
	if !memberPresent(t, ctx, chB, "fixture_client") {
		t.Fatal("static fixture member was reaped, want it to survive indefinitely")
	}
}

// swapPresenceTimings overrides the presence lease/bump/reaper vars and
// returns a restore func.
func swapPresenceTimings(lease, bump, reap time.Duration) func() {
	ol, ob, or := presenceLeaseWindow, presenceLeaseBumpInterval, presenceReaperInterval
	presenceLeaseWindow, presenceLeaseBumpInterval, presenceReaperInterval = lease, bump, reap
	return func() {
		presenceLeaseWindow, presenceLeaseBumpInterval, presenceReaperInterval = ol, ob, or
	}
}

func memberPresent(t *testing.T, ctx context.Context, ch storage.ChannelStore, clientID string) bool {
	t.Helper()
	chPage, err := ch.Members(ctx, storage.MembersQuery{})
	members, _ := chPage.Members, chPage.AsOfSerial
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	for _, m := range members {
		if m.GetClientId() == clientID {
			return true
		}
	}
	return false
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// presenceRecorder is a storage.Appender that records delivered presence
// cms so tests can count LEAVEs per client.
type presenceRecorder struct {
	mu       sync.Mutex
	presence []*wire.PresenceMessage
}

func (r *presenceRecorder) Initialize(current, initial string) {}

// OccupancyChanged is not what this recorder is watching for.
func (r *presenceRecorder) OccupancyChanged() {}

func (r *presenceRecorder) Append(cm *protocol.ChannelMessage) {
	r.mu.Lock()
	r.presence = append(r.presence, cm.Presence...)
	r.mu.Unlock()
}

func (r *presenceRecorder) leaveCount(clientID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, p := range r.presence {
		if p.Action == wire.PresenceMessage_LEAVE && p.GetClientId() == clientID {
			n++
		}
	}
	return n
}
