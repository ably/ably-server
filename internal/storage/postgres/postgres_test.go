//go:build integration

package postgres_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
	"github.com/ably/ably-server/internal/storage/storagetest"
)

func TestPostgresChannelStoreContract(t *testing.T) {
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
	want := []string{"0001_initial", "0002_channels_and_serial_mint", "0003_channels_initial_serial", "0004_presence"}
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

// TestPostgresClusterBrokerDeliversCrossNode brings up two
// postgres.Storage instances against the same schema (i.e. two
// "nodes" of a cluster), registers a recording Appender on each via
// Channel(name, appender), publishes through one node's
// ChannelStore.Store, and asserts that BOTH nodes' appenders receive
// the cm via the LISTEN/NOTIFY round-trip. The publisher's own
// receipt arrives via the same NOTIFY path (no self-dedup) — that's
// the unified flow.
func TestPostgresClusterBrokerDeliversCrossNode(t *testing.T) {
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
	cm, idempotent, err := ch1.Store(ctx, []*protocol.Message{{ID: "m1", Data: "hi"}})
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
	if len(got1.Messages) != 1 || got1.Messages[0].ID != "m1" {
		t.Errorf("node1 appender payload = %+v, want one Message with ID m1", got1.Messages)
	}
	if len(got2.Messages) != 1 || got2.Messages[0].ID != "m1" {
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
			cm, _, err := ch1.Store(ctx, []*protocol.Message{{Name: "x"}})
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
			cm, _, err := ch2.Store(ctx, []*protocol.Message{{Name: "y"}})
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

// recordingAppender captures every cm passed to Append, exposing
// wait() for tests that need to synchronise on delivery.
type recordingAppender struct {
	mu  sync.Mutex
	cms []*protocol.ChannelMessage
	got chan *protocol.ChannelMessage
}

func newRecordingAppender() *recordingAppender {
	return &recordingAppender{got: make(chan *protocol.ChannelMessage, 16)}
}

func (a *recordingAppender) Initialize(current, initial string) {
	// noop for this test: we only assert on Append delivery.
	_, _ = current, initial
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
