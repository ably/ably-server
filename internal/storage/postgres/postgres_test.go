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
	if !slices.Equal(versions, []string{"0001_initial"}) {
		t.Errorf("schema_migrations rows = %v, want [0001_initial]", versions)
	}

	// The messages table must exist and be queryable.
	if _, err := conn.Exec(ctx, `SELECT 1 FROM messages LIMIT 0`); err != nil {
		t.Errorf("messages table not present: %v", err)
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
	ch1 := s1.Channel("room", a1)
	_ = s2.Channel("room", a2)

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
