//go:build integration

package main

import (
	"context"
	"net"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/ably/ably-go/ably"

	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// newClientWithID builds an ably-go realtime client with a clientId,
// required to enter presence. With Basic auth ably-go sends the clientId
// as the connection's ?clientId= query param (RSA7e1), which the server
// resolves per DESIGN.md §3.2. clientID == "" yields an anonymous client.
func newClientWithID(t *testing.T, addr, clientID string) *ably.Realtime {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split host:port %q: %v", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("port %q: %v", portStr, err)
	}
	opts := []ably.ClientOption{
		ably.WithKey(integrationAPIKey),
		ably.WithEndpoint(host),
		ably.WithPort(port),
		ably.WithTLS(false),
		ably.WithInsecureAllowBasicAuthWithoutTLS(),
		ably.WithUseTokenAuth(false),
		ably.WithAutoConnect(false),
		ably.WithRealtimeRequestTimeout(5 * time.Second),
		ably.WithLogLevel(ably.LogNone),
	}
	if clientID != "" {
		opts = append(opts, ably.WithClientID(clientID))
	}
	client, err := ably.NewRealtime(opts...)
	if err != nil {
		t.Fatalf("NewRealtime: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// waitPresenceSet polls Presence.Get until the set of member clientIds
// equals want (order-insensitive), or the context deadline fires. Used
// to wait for cross-node propagation without asserting on timing.
func waitPresenceSet(t *testing.T, ctx context.Context, ch *ably.RealtimeChannel, want ...string) {
	t.Helper()
	sort.Strings(want)
	for {
		members, err := ch.Presence.Get(ctx)
		if err != nil {
			t.Fatalf("Presence.Get: %v", err)
		}
		if got := clientIDsOf(members); sameStrings(got, want) {
			return
		}
		select {
		case <-ctx.Done():
			members, _ := ch.Presence.Get(context.Background())
			t.Fatalf("presence set = %v, want %v (timed out: %v)", clientIDsOf(members), want, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func clientIDsOf(members []*ably.PresenceMessage) []string {
	seen := make(map[string]struct{}, len(members))
	for _, m := range members {
		seen[m.ClientID] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestIntegrationClusterPresenceSyncAcrossNodes: a member entered on one
// node is visible — via cross-node SYNC — to a client attaching on
// another node, and once both have entered, both nodes report the same
// set. Covers TASK-48 ACs #1 and #3.
func TestIntegrationClusterPresenceSyncAcrossNodes(t *testing.T) {
	pgc := pgtest.Start(t)
	dsn := pgc.FreshSchemaDSN(t)
	addrA := startServerOnDSN(t, dsn)
	addrB := startServerOnDSN(t, dsn)

	ctx, cancel := testCtx(t)
	defer cancel()

	// alice enters on node A.
	a := newClientWithID(t, addrA, "alice")
	connect(t, a)
	chA := a.Channels.Get("room")
	if err := chA.Presence.Enter(ctx, "hi"); err != nil {
		t.Fatalf("alice enter: %v", err)
	}

	// A client on node B sees alice via cross-node SYNC (AC #1).
	b := newClientWithID(t, addrB, "bob")
	connect(t, b)
	chB := b.Channels.Get("room")
	waitPresenceSet(t, ctx, chB, "alice")

	// bob enters on node B; both nodes converge to {alice, bob} (AC #3).
	if err := chB.Presence.Enter(ctx, "yo"); err != nil {
		t.Fatalf("bob enter: %v", err)
	}
	waitPresenceSet(t, ctx, chA, "alice", "bob")
	waitPresenceSet(t, ctx, chB, "alice", "bob")
}

// TestIntegrationClusterPresenceEventsAcrossNodes: enter/update/leave
// published on one node are delivered, in order, to a presence
// subscriber on another node via the NOTIFY broker. Covers TASK-48 AC #2.
func TestIntegrationClusterPresenceEventsAcrossNodes(t *testing.T) {
	pgc := pgtest.Start(t)
	dsn := pgc.FreshSchemaDSN(t)
	addrA := startServerOnDSN(t, dsn)
	addrB := startServerOnDSN(t, dsn)

	ctx, cancel := testCtx(t)
	defer cancel()

	// Subscriber on node B, attached before any presence activity so the
	// channel is registered on B and its LISTEN delivery is live.
	b := newClientWithID(t, addrB, "observer")
	connect(t, b)
	chB := b.Channels.Get("room")
	if err := chB.Attach(ctx); err != nil {
		t.Fatalf("B attach: %v", err)
	}
	events := make(chan *ably.PresenceMessage, 8)
	unsub, err := chB.Presence.SubscribeAll(ctx, func(m *ably.PresenceMessage) {
		events <- m
	})
	if err != nil {
		t.Fatalf("B SubscribeAll: %v", err)
	}
	defer unsub()

	// alice's lifecycle on node A.
	a := newClientWithID(t, addrA, "alice")
	connect(t, a)
	chA := a.Channels.Get("room")
	if err := chA.Presence.Enter(ctx, "1"); err != nil {
		t.Fatalf("alice enter: %v", err)
	}
	if err := chA.Presence.Update(ctx, "2"); err != nil {
		t.Fatalf("alice update: %v", err)
	}
	if err := chA.Presence.Leave(ctx, "3"); err != nil {
		t.Fatalf("alice leave: %v", err)
	}

	// B observes enter, update, leave for alice, in order.
	for i, want := range []ably.PresenceAction{
		ably.PresenceActionEnter, ably.PresenceActionUpdate, ably.PresenceActionLeave,
	} {
		select {
		case m := <-events:
			if m.ClientID != "alice" {
				t.Errorf("event %d clientId = %q, want alice", i, m.ClientID)
			}
			if m.Action != want {
				t.Errorf("event %d action = %v, want %v", i, m.Action, want)
			}
		case <-ctx.Done():
			t.Fatalf("event %d: timed out waiting for %v (%v)", i, want, ctx.Err())
		}
	}
}
