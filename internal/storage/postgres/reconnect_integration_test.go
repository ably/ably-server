//go:build integration

package postgres

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// TestListenReconnectReconcilesGap forces the LISTEN connection to drop
// mid-stream, publishes cms during the gap (whose NOTIFYs are lost),
// and asserts the appender still observes every cm — pre-gap, in-gap,
// and post-gap — exactly once and in channelSerial order.
func TestListenReconnectReconcilesGap(t *testing.T) {
	// Shrink the reconnect backoff so the test runs in ms, not seconds.
	defer swapReconnectDelays(20*time.Millisecond, 100*time.Millisecond)()

	c := pgtest.Start(t)
	appName := fmt.Sprintf("task29_%d", time.Now().UnixNano())
	dsn := withApplicationName(t, c.FreshSchemaDSN(t), appName)
	ctx := context.Background()

	s, err := Open(ctx, Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	app := &recorder{}
	ch, err := s.Channel(ctx, "room", app)
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}

	var want []string

	// Pre-gap publishes — wait until they are delivered so the
	// high-water mark is advanced before we drop the conn.
	for i := 0; i < 5; i++ {
		want = append(want, publish(t, ctx, ch, fmt.Sprintf("pre-%d", i)))
	}
	waitForCount(t, app, len(want), 10*time.Second)

	// Force the LISTEN backend to drop.
	terminateListenBackend(t, c.BaseDSN(), appName)

	// In-gap publishes — their NOTIFYs are likely lost while the conn
	// is down; reconcile must replay them.
	for i := 0; i < 5; i++ {
		want = append(want, publish(t, ctx, ch, fmt.Sprintf("gap-%d", i)))
	}

	// Post-gap publishes — should flow via the normal NOTIFY path once
	// the conn is back, deduped against any reconcile overlap.
	for i := 0; i < 5; i++ {
		want = append(want, publish(t, ctx, ch, fmt.Sprintf("post-%d", i)))
	}

	waitForCount(t, app, len(want), 15*time.Second)

	got := app.serials()
	if len(got) != len(want) {
		t.Fatalf("appender saw %d cms, want %d (%v)", len(got), len(want), got)
	}
	for i, s := range got {
		if s != want[i] {
			t.Fatalf("cm[%d] = %q, want %q — order/dedup broken\n got=%v\nwant=%v", i, s, want[i], got, want)
		}
	}
}

func publish(t *testing.T, ctx context.Context, ch storage.ChannelStore, data string) string {
	t.Helper()
	cm, _, err := ch.Store(ctx, []*protocol.Message{{Data: data}})
	if err != nil {
		t.Fatalf("Store %q: %v", data, err)
	}
	return cm.ChannelSerial
}

// terminateListenBackend kills the backend running the broker LISTEN for
// the given application_name, simulating a dropped conn from outside.
func terminateListenBackend(t *testing.T, baseDSN, appName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect to terminate LISTEN backend: %v", err)
	}
	defer conn.Close(context.Background())

	tag, err := conn.Exec(ctx,
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		 WHERE application_name = $1 AND query LIKE 'LISTEN%'`, appName)
	if err != nil {
		t.Fatalf("pg_terminate_backend: %v", err)
	}
	if tag.RowsAffected() == 0 {
		t.Fatalf("no LISTEN backend found for application_name=%q", appName)
	}
}

// swapReconnectDelays overrides the package reconnect-backoff vars and
// returns a restore func.
func swapReconnectDelays(base, max time.Duration) func() {
	origBase, origMax := listenReconnectBaseDelay, listenReconnectMaxDelay
	listenReconnectBaseDelay, listenReconnectMaxDelay = base, max
	return func() {
		listenReconnectBaseDelay, listenReconnectMaxDelay = origBase, origMax
	}
}

// withApplicationName appends application_name to a URL-form DSN so the
// test can find (and terminate) this node's backends in pg_stat_activity.
func withApplicationName(t *testing.T, dsn, name string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	q := u.Query()
	q.Set("application_name", name)
	u.RawQuery = q.Encode()
	return u.String()
}

func waitForCount(t *testing.T, r *recorder, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if r.count() >= n {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d cms; have %d", n, r.count())
}

// recorder is a storage.Appender that records the channelSerial of every
// delivered cm in arrival order.
type recorder struct {
	mu  sync.Mutex
	cms []string
}

func (r *recorder) Initialize(current, initial string) {}

func (r *recorder) Append(cm *protocol.ChannelMessage) {
	r.mu.Lock()
	r.cms = append(r.cms, cm.ChannelSerial)
	r.mu.Unlock()
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.cms)
}

func (r *recorder) serials() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.cms))
	copy(out, r.cms)
	return out
}
