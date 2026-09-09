package postgres_test

import (
	"context"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/integration"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/serial"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
	"github.com/ably/ably-server/internal/storage/storagetest"
	"github.com/ably/server-protocol/go/logging"
	"github.com/ably/server-protocol/go/wire"
)

func TestPostgresChannelStoreContract(t *testing.T) {
	integration.Require(t)
	c := pgtest.Start(t)
	storagetest.RunChannelStoreTests(t, func(t *testing.T) storage.Storage {
		dsn := c.FreshSchemaDSN(t)
		s, err := postgres.Open(context.Background(), postgres.Options{DSN: dsn})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

func TestPostgresBootstrapIsIdempotent(t *testing.T) {
	integration.Require(t)
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)

	// First Open creates the schema; second Open should observe it
	// already exists and be a no-op.
	s1, err := postgres.Open(context.Background(), postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	s2, err := postgres.Open(context.Background(), postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open #2 (idempotent bootstrap): %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })
}

// TestPostgresMigrateIsConcurrentSafe spawns N goroutines that each
// call postgres.Open on the same fresh schema simultaneously. The
// session-scoped pg_advisory_lock should serialise the migration
// sweep: all Opens succeed; schema_migrations ends up with exactly
// one row per shipped migration (no duplicates from concurrent
// inserts); the messages table is correctly created.
func TestPostgresMigrateIsConcurrentSafe(t *testing.T) {
	integration.Require(t)
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	const workers = 10
	var (
		wg   sync.WaitGroup
		errs = make(chan error, workers)
	)
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			s, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
			if err != nil {
				errs <- err
				return
			}
			_ = s.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent Open: %v", err)
	}

	// schema_migrations must have exactly one row per shipped
	// migration — no duplicates, no partial application.
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for verification: %v", err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatalf("read schema_migrations: %v", err)
	}
	var versions []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	want := shippedMigrations(t)
	if !slices.Equal(versions, want) {
		t.Errorf("schema_migrations rows = %v, want %v", versions, want)
	}

	// The log, channels, and presence tables must exist and be queryable.
	if _, err := conn.Exec(ctx, `SELECT 1 FROM channel_messages LIMIT 0`); err != nil {
		t.Errorf("channel_messages table not present: %v", err)
	}
	if _, err := conn.Exec(ctx, `SELECT 1 FROM presence LIMIT 0`); err != nil {
		t.Errorf("presence table not present: %v", err)
	}
	if _, err := conn.Exec(ctx, `SELECT 1 FROM channels LIMIT 0`); err != nil {
		t.Errorf("channels table not present: %v", err)
	}
}

// shippedMigrations returns the versions of the migrations in the
// package's migrations/ directory, in the order Open applies them. Read
// from disk rather than listed here, so shipping a migration does not
// mean remembering to name it in a test.
func shippedMigrations(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	var versions []string
	for _, e := range entries {
		if name := e.Name(); strings.HasSuffix(name, ".sql") {
			versions = append(versions, strings.TrimSuffix(name, ".sql"))
		}
	}
	slices.Sort(versions)
	return versions
}

// TestPostgresClusterBrokerDeliversCrossNode brings up two
// postgres.Storage instances against the same schema (i.e. two
// "nodes" of a cluster), registers a recording Appender on each via
// Channel(name, appender), publishes through one node's
// ChannelStore.Store, and asserts that BOTH nodes' appenders receive
// the cm via the LISTEN/NOTIFY round-trip. The publisher's own
// receipt arrives via the same NOTIFY path (no self-dedup) — that's
// the unified flow.
func TestPostgresClusterBrokerDeliversCrossNode(t *testing.T) {
	integration.Require(t)
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s1, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open node1: %v", err)
	}
	t.Cleanup(func() { _ = s1.Close() })

	s2, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open node2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	a1 := newRecordingAppender()
	a2 := newRecordingAppender()
	ch1, err := s1.Channel(ctx, "room", a1)
	if err != nil {
		t.Fatalf("Channel node1: %v", err)
	}
	if _, err := s2.Channel(ctx, "room", a2); err != nil {
		t.Fatalf("Channel node2: %v", err)
	}

	// Publish via node1 only. Both nodes' appenders should observe.
	cm, idempotent, err := ch1.Store(ctx, []*wire.Message{{Id: new("m1"), Data: wire.MessageStrData("hi")}})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if idempotent {
		t.Fatal("idempotent=true on fresh publish")
	}

	got1 := a1.wait(t, 3*time.Second)
	got2 := a2.wait(t, 3*time.Second)
	if got1.ChannelSerial != cm.ChannelSerial {
		t.Errorf("node1 appender saw %q, want %q", got1.ChannelSerial, cm.ChannelSerial)
	}
	if got2.ChannelSerial != cm.ChannelSerial {
		t.Errorf("node2 appender saw %q, want %q", got2.ChannelSerial, cm.ChannelSerial)
	}
	if len(got1.Messages) != 1 || got1.Messages[0].GetId() != "m1" {
		t.Errorf("node1 appender payload = %+v, want one Message with ID m1", got1.Messages)
	}
	if len(got2.Messages) != 1 || got2.Messages[0].GetId() != "m1" {
		t.Errorf("node2 appender payload = %+v, want one Message with ID m1", got2.Messages)
	}

	// Each appender should have received exactly one cm — the
	// publisher does not link synchronously, so we must not have a
	// second arrival from a "direct" path.
	if got := a1.count(); got != 1 {
		t.Errorf("node1 appender call count = %d, want 1 (no duplicates from self-NOTIFY)", got)
	}
	if got := a2.count(); got != 1 {
		t.Errorf("node2 appender call count = %d, want 1", got)
	}
}

// TestPostgresClusterSerialsAreStrictlyMonotonic publishes concurrently
// from two postgres.Storage instances against the same database and
// asserts the resulting channel_serials are strictly increasing in the
// commit order. This is the property the channels-row mint function
// guarantees that the previous process-local Generator could not
// (DESIGN.md §8): two nodes minting at the same wall-clock ms used to
// be disambiguated only by seriesId, which could produce serials that
// lex-ordered out of commit order.
func TestPostgresClusterSerialsAreStrictlyMonotonic(t *testing.T) {
	integration.Require(t)
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s1, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open node1: %v", err)
	}
	t.Cleanup(func() { _ = s1.Close() })
	s2, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open node2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	ch1, err := s1.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel node1: %v", err)
	}
	ch2, err := s2.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel node2: %v", err)
	}

	const perNode = 100
	serialsCh := make(chan string, 2*perNode)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range perNode {
			cm, _, err := ch1.Store(ctx, []*wire.Message{{Name: new("x")}})
			if err != nil {
				t.Errorf("ch1 Store: %v", err)
				return
			}
			serialsCh <- cm.ChannelSerial
		}
	}()
	go func() {
		defer wg.Done()
		for range perNode {
			cm, _, err := ch2.Store(ctx, []*wire.Message{{Name: new("y")}})
			if err != nil {
				t.Errorf("ch2 Store: %v", err)
				return
			}
			serialsCh <- cm.ChannelSerial
		}
	}()
	wg.Wait()
	close(serialsCh)

	// Pull the canonical order from the DB rather than relying on
	// per-goroutine arrival order. With the channels-row mint, the
	// row's serial advances strictly on each successful publish — so
	// the rows in messages are in mint order.
	page, err := ch1.History(ctx, storage.HistoryQuery{Direction: storage.DirectionForwards, Limit: 1000})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if got := len(page.ChannelMessages); got != 2*perNode {
		t.Fatalf("history len = %d, want %d", got, 2*perNode)
	}
	var prev string
	for i, cm := range page.ChannelMessages {
		if cm.ChannelSerial <= prev {
			t.Fatalf("page[%d] serial %q not > previous %q — cluster mint is not strictly monotonic", i, cm.ChannelSerial, prev)
		}
		prev = cm.ChannelSerial
	}

	// Sanity: every produced serial appears in history exactly once.
	seen := make(map[string]int, 2*perNode)
	for s := range serialsCh {
		seen[s]++
	}
	for _, cm := range page.ChannelMessages {
		if seen[cm.ChannelSerial] != 1 {
			t.Errorf("history serial %q seen %d times in publish outputs, want 1", cm.ChannelSerial, seen[cm.ChannelSerial])
		}
	}
}

// TestPostgresChannelSeriesIsTheChannelsNotTheNodes publishes to one
// channel from two nodes and asserts every serial carries the same
// series (DESIGN.md §8).
//
// The series is how a server tells a client that the serials before it
// are not ordered against the ones after it. Minting with the
// publishing node's own series made a channel appear to restart its
// ordering every time the node behind a publish changed — on a channel
// nothing had happened to.
func TestPostgresChannelSeriesIsTheChannelsNotTheNodes(t *testing.T) {
	integration.Require(t)
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s1, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open node1: %v", err)
	}
	t.Cleanup(func() { _ = s1.Close() })
	s2, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open node2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	ch1, err := s1.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel node1: %v", err)
	}
	ch2, err := s2.Channel(ctx, "room", nil)
	if err != nil {
		t.Fatalf("Channel node2: %v", err)
	}

	// Alternate the publishing node, which is what a handover looks
	// like to the channel.
	var serials []string
	for i := range 6 {
		ch := ch1
		if i%2 == 1 {
			ch = ch2
		}
		cm, _, err := ch.Store(ctx, []*wire.Message{{Name: new("x")}})
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		serials = append(serials, cm.ChannelSerial)
	}

	_, _, want, err := serial.SplitChannelSerial(serials[0])
	if err != nil {
		t.Fatalf("split %q: %v", serials[0], err)
	}
	for i, s := range serials {
		_, _, got, err := serial.SplitChannelSerial(s)
		if err != nil {
			t.Fatalf("split %q: %v", s, err)
		}
		if got != want {
			t.Errorf("serial %d (%q) is in series %q, want %q — the channel changed series on a node handover",
				i, s, got, want)
		}
	}
}

// TestPostgresClusterSummaryIsCrossNodeDeterministic verifies that a node
// which never computed a fold still reads the summary it produced (DESIGN.md
// §14.2). The fold lands on the messages projection inside the same
// transaction as the annotation write, so the summary is a fact about the
// message in the database rather than something a node has to have witnessed
// the history to know — node2 reads it having only ever seen the NOTIFY.
func TestPostgresClusterSummaryIsCrossNodeDeterministic(t *testing.T) {
	integration.Require(t)
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()

	s1, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open node1: %v", err)
	}
	t.Cleanup(func() { _ = s1.Close() })
	s2, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open node2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	a1 := newRecordingAppender()
	a2 := newRecordingAppender()
	ch1, err := s1.Channel(ctx, "room", a1)
	if err != nil {
		t.Fatalf("Channel node1: %v", err)
	}
	ch2, err := s2.Channel(ctx, "room", a2)
	if err != nil {
		t.Fatalf("Channel node2: %v", err)
	}

	// Publish a target message via node1 and let both nodes observe it, so
	// node2's projection has the target before the annotation lands.
	target, _, err := ch1.Store(ctx, []*wire.Message{{Id: new("m1"), Data: wire.MessageStrData("post")}})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	a1.waitAnnotationOrMessage(t, 3*time.Second)
	a2.waitAnnotationOrMessage(t, 3*time.Second)
	targetSerial := target.Messages[0].Serial

	// Two annotations from distinct clients on node1: only node1 computes the
	// fold.
	for _, client := range []string{"alice", "bob"} {
		if _, _, err := ch1.StoreAnnotation(ctx, []*wire.Annotation{{
			Action: wire.Annotation_ANNOTATION_CREATE, ClientId: new(client),
			Type: "reaction:distinct.v1", Name: "👍", MessageSerial: targetSerial,
		}}, core.FoldSummary(logging.Nop)); err != nil {
			t.Fatalf("StoreAnnotation %s: %v", client, err)
		}
	}

	// Both nodes see both annotation cms.
	for range 2 {
		waitForAnnotation(t, a1, 3*time.Second)
		waitForAnnotation(t, a2, 3*time.Second)
	}

	// Each node reads the same summary off the message, including the one that
	// only ever received the NOTIFY.
	for node, ch := range map[string]storage.ChannelStore{"node1": ch1, "node2": ch2} {
		m, err := ch.LatestVersion(ctx, targetSerial)
		if err != nil {
			t.Fatalf("%s LatestVersion: %v", node, err)
		}
		agg := m.GetAnnotations().GetSummary()["reaction:distinct.v1"]
		list := agg.GetDistinctV1().GetValues()["👍"]
		if list == nil || !slices.Equal(list.ClientIds, []string{"alice", "bob"}) {
			t.Errorf("%s summary = %#v, want 👍:[alice,bob]", node, m.GetAnnotations().GetSummary())
		}
	}
}

// waitForAnnotation drains the appender until an annotation cm arrives and
// returns its first annotation.
func waitForAnnotation(t *testing.T, a *recordingAppender, timeout time.Duration) *wire.Annotation { //nolint:unparam
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case cm := <-a.got:
			if len(cm.Annotations) > 0 {
				return cm.Annotations[0]
			}
		case <-deadline:
			t.Fatalf("no annotation cm within %s", timeout)
			return nil
		}
	}
}

// waitAnnotationOrMessage drains one cm of any kind (used to synchronise on
// the target message's cross-node delivery).
func (a *recordingAppender) waitAnnotationOrMessage(t *testing.T, timeout time.Duration) {
	t.Helper()
	a.wait(t, timeout)
}

// recordingAppender captures every cm passed to Append, exposing
// wait() for tests that need to synchronise on delivery.
type recordingAppender struct {
	mu               sync.Mutex
	cms              []*protocol.ChannelMessage
	occupancyChanges int
	got              chan *protocol.ChannelMessage
}

func newRecordingAppender() *recordingAppender {
	return &recordingAppender{got: make(chan *protocol.ChannelMessage, 16)}
}

func (a *recordingAppender) Initialize(current, initial string) {
	// noop for this test: we only assert on Append delivery.
	_, _ = current, initial
}

// OccupancyChanged counts the occupancy signals this node received, which is
// how a cross-node test tells that another node's contribution reached it.
func (a *recordingAppender) OccupancyChanged() {
	a.mu.Lock()
	a.occupancyChanges++
	a.mu.Unlock()
}

func (a *recordingAppender) occupancyChangeCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.occupancyChanges
}

func (a *recordingAppender) Append(cm *protocol.ChannelMessage) {
	a.mu.Lock()
	a.cms = append(a.cms, cm)
	a.mu.Unlock()
	a.got <- cm
}

func (a *recordingAppender) wait(t *testing.T, timeout time.Duration) *protocol.ChannelMessage {
	t.Helper()
	select {
	case cm := <-a.got:
		return cm
	case <-time.After(timeout):
		t.Fatalf("appender did not receive within %s", timeout)
		return nil
	}
}

func (a *recordingAppender) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.cms)
}

// TestPostgresClusterOccupancyAggregatesAcrossNodes is the cluster case
// occupancy exists for: two nodes each serving part of a channel's
// attachments, and either node able to answer what the whole channel holds.
//
// It checks both halves of that. The aggregate one node reads includes the
// other's contribution — so the sum is genuinely across nodes and not just a
// read of local counts — and the node that did not write is told the aggregate
// moved, without which nothing would ever prompt it to re-read.
func TestPostgresClusterOccupancyAggregatesAcrossNodes(t *testing.T) {
	integration.Require(t)
	ctx := context.Background()
	c := pgtest.Start(t)
	dsn := c.FreshSchemaDSN(t)

	s1, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open node1: %v", err)
	}
	t.Cleanup(func() { _ = s1.Close() })
	s2, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open node2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	a1, a2 := newRecordingAppender(), newRecordingAppender()
	ch1, err := s1.Channel(ctx, "room", a1)
	if err != nil {
		t.Fatalf("Channel node1: %v", err)
	}
	ch2, err := s2.Channel(ctx, "room", a2)
	if err != nil {
		t.Fatalf("Channel node2: %v", err)
	}

	// node1 serves two subscribers, node2 one publisher.
	if err := ch1.StoreOccupancy(ctx, &wire.ChannelOccupancy{
		ChannelMode: 1 << 18, Connections: 2, Subscribers: 2,
	}); err != nil {
		t.Fatalf("node1 StoreOccupancy: %v", err)
	}
	if err := ch2.StoreOccupancy(ctx, &wire.ChannelOccupancy{
		ChannelMode: 1 << 17, Connections: 1, Publishers: 1,
	}); err != nil {
		t.Fatalf("node2 StoreOccupancy: %v", err)
	}

	// Both nodes read the same whole-channel occupancy: the counts summed and
	// the modes unioned.
	for node, ch := range map[string]storage.ChannelStore{"node1": ch1, "node2": ch2} {
		occ, err := ch.Occupancy(ctx)
		if err != nil {
			t.Fatalf("%s Occupancy: %v", node, err)
		}
		if occ.GetConnections() != 3 {
			t.Errorf("%s connections = %d, want 3 (2 on node1 + 1 on node2)", node, occ.GetConnections())
		}
		if occ.GetSubscribers() != 2 || occ.GetPublishers() != 1 {
			t.Errorf("%s occupancy = %+v, want 2 subscribers and 1 publisher", node, occ)
		}
		if want := int32(1<<18 | 1<<17); occ.GetChannelMode() != want {
			t.Errorf("%s channelMode = %d, want the union %d", node, occ.GetChannelMode(), want)
		}
	}

	// And each node was told the aggregate moved — node2 by node1's write as
	// much as by its own, since it hears every write on the channel.
	deadline := time.Now().Add(10 * time.Second)
	for (a1.occupancyChangeCount() < 2 || a2.occupancyChangeCount() < 2) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := a1.occupancyChangeCount(); got < 2 {
		t.Errorf("node1 was signalled %d times, want both writes", got)
	}
	if got := a2.occupancyChangeCount(); got < 2 {
		t.Errorf("node2 was signalled %d times, want both writes", got)
	}

	// A node withdrawing takes only its own share out.
	if err := ch1.StoreOccupancy(ctx, nil); err != nil {
		t.Fatalf("node1 withdraw: %v", err)
	}
	occ, err := ch2.Occupancy(ctx)
	if err != nil {
		t.Fatalf("node2 Occupancy after node1 withdrew: %v", err)
	}
	if occ.GetConnections() != 1 || occ.GetPublishers() != 1 || occ.GetSubscribers() != 0 {
		t.Errorf("occupancy = %+v after node1 withdrew, want node2's publisher alone", occ)
	}
}
