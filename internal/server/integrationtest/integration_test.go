//go:build integration

// Package integrationtest holds the SDK-driven integration suite: it
// boots ably-server in-process (via internal/server.Run) and drives it
// with the real ably-go realtime/REST client, exercising end-to-end
// flows (publish/subscribe, presence, mutable-message versions, and
// multi-node cluster fan-out) that unit tests below internal/server
// can't reach. It's behind the "integration" build tag because the
// cluster-mode tests need a real Postgres (via
// internal/storage/postgres/pgtest, i.e. a Docker testcontainer).
//
// Run it with:
//
//	go test -tags=integration ./...
package integrationtest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ably/ably-go/ably"

	"github.com/ably/ably-server/internal/server"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

const integrationAPIKey = "app.key:secret"

// startServer boots a single ably-server instance in cluster mode
// against a fresh Postgres schema, on a free TCP port discovered via
// the Ready hook in runOpts. It returns the bound "host:port" string;
// the server is torn down on t.Cleanup.
//
// For multi-node tests use startServerOnDSN with a shared
// FreshSchemaDSN so every node speaks to the same database state.
func startServer(t *testing.T, extraArgs ...string) string {
	t.Helper()
	pgc := pgtest.Start(t)
	return startServerOnDSN(t, pgc.FreshSchemaDSN(t), extraArgs...)
}

// editableChannelsConfig writes a config file declaring the named namespaces
// with message editing allowed, and returns its path.
//
// A namespace that does not say messages are mutable does not allow editing
// them (DESIGN.md §13), so a server started with no namespaces configured
// refuses every edit. That is the protocol's answer rather than this server's,
// so a test about editing configures a namespace that permits it, exactly as a
// test app does. An unprefixed channel's namespace is its own name, so the
// namespaces to declare are the channel names the test uses.
func editableChannelsConfig(t *testing.T, namespaces ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ably-server.toml")
	var body strings.Builder
	for _, ns := range namespaces {
		fmt.Fprintf(&body, "[[namespaces]]\nid = %q\nmutableMessages = true\n\n", ns)
	}
	if err := os.WriteFile(path, []byte(body.String()), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// startServerOnDSN boots one ably-server in cluster mode pointed at
// the given DSN. The DSN may be a fresh schema (single-node test) or
// a schema shared with sibling nodes (cluster test). Returns the
// bound "host:port"; tears down on t.Cleanup.
func startServerOnDSN(t *testing.T, dsn string, extraArgs ...string) string {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	ready := make(chan net.Addr, 1)
	done := make(chan int, 1)
	go func() {
		done <- server.Run(ctx, server.Opts{
			Args: append([]string{
				"--keys=" + integrationAPIKey,
				"--mode=cluster",
				"--postgres-dsn=" + dsn,
				"--listen=127.0.0.1:0",
				"--log-level=error",
			}, extraArgs...),
			Getenv: func(string) string { return "" },
			Out:    io.Discard,
			Ready:  ready,
		})
	}()

	var addr net.Addr
	select {
	case addr = <-ready:
	case code := <-done:
		t.Fatalf("server exited before ready (code=%d)", code)
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatal("server did not become ready within 30s")
	}

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("server did not shut down within 10s")
		}
	})

	return addr.String()
}

// newClient builds an anonymous ably-go realtime client pointed at the
// running server. The client is closed on t.Cleanup. For presence tests
// that need a clientId, use newClientWithID.
func newClient(t *testing.T, addr string) *ably.Realtime {
	t.Helper()
	return newClientWithID(t, addr, "")
}

// connect drives the client to CONNECTED.
func connect(t *testing.T, client *ably.Realtime) {
	t.Helper()
	connected := make(chan struct{}, 1)
	client.Connection.Once(ably.ConnectionEventConnected, func(ably.ConnectionStateChange) {
		connected <- struct{}{}
	})
	client.Connect()
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for CONNECTED; current state: %v", client.Connection.State())
	}
}

// testCtx returns a context bounded by t.Deadline() (with a 1s safety
// margin) or 5s if -timeout is not set. The bound is the "test never
// hangs" guard, not an expected-arrival window.
func testCtx(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	if deadline, ok := t.Deadline(); ok {
		return context.WithDeadline(context.Background(), deadline.Add(-time.Second))
	}
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// postPublish sends one REST publish to the running server.
func postPublish(t *testing.T, addr, channel, jsonBody string) {
	t.Helper()
	u := &url.URL{
		Scheme: "http",
		Host:   addr,
		Path:   "/channels/" + channel + "/messages",
	}
	req, err := http.NewRequest(http.MethodPost, u.String(), strings.NewReader(jsonBody))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("app.key", "secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("REST publish: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("REST publish status = %d, want 201; body=%s", resp.StatusCode, bytes.TrimSpace(body))
	}
}

// TestIntegrationRESTPublishToWSSubscribe boots ably-server against a
// real Postgres testcontainer, opens a WS subscription via the SDK,
// publishes via REST, and asserts the WS subscriber receives the
// message. Exercises the full end-to-end path:
// REST → core.Channel.Publish → postgres.Store → NOTIFY → LISTEN
// goroutine → Appender → core.Channel.Append → realtime attachment →
// WS MESSAGE frame.
func TestIntegrationRESTPublishToWSSubscribe(t *testing.T) {
	addr := startServer(t)
	client := newClient(t, addr)
	connect(t, client)

	ctx, cancel := testCtx(t)
	defer cancel()

	ch := client.Channels.Get("foo")
	received := make(chan *ably.Message, 4)
	unsub, err := ch.SubscribeAll(ctx, func(m *ably.Message) {
		received <- m
	})
	if err != nil {
		t.Fatalf("SubscribeAll: %v", err)
	}
	defer unsub()

	postPublish(t, addr, "foo", `{"name":"greeting","data":"hello"}`)

	select {
	case m := <-received:
		if m.Name != "greeting" {
			t.Errorf("received Name = %q, want %q", m.Name, "greeting")
		}
		if got, ok := m.Data.(string); !ok || got != "hello" {
			t.Errorf("received Data = %v, want %q", m.Data, "hello")
		}
	case <-ctx.Done():
		t.Fatalf("subscriber did not receive REST publish before deadline (%v)", ctx.Err())
	}
}

// TestIntegrationWSPublishSelfLoop publishes via the SDK and asserts
// the same client receives its own message — proving the unified
// delivery path goes through the LISTEN/NOTIFY round-trip even for
// the publisher's own publishes.
func TestIntegrationWSPublishSelfLoop(t *testing.T) {
	addr := startServer(t)
	client := newClient(t, addr)
	connect(t, client)

	ctx, cancel := testCtx(t)
	defer cancel()

	ch := client.Channels.Get("bar")
	received := make(chan *ably.Message, 4)
	unsub, err := ch.SubscribeAll(ctx, func(m *ably.Message) {
		received <- m
	})
	if err != nil {
		t.Fatalf("SubscribeAll: %v", err)
	}
	defer unsub()

	if err := ch.Publish(ctx, "echo", "pong"); err != nil {
		t.Fatalf("Publish (proves ACK): %v", err)
	}

	select {
	case m := <-received:
		if m.Name != "echo" {
			t.Errorf("received Name = %q, want %q", m.Name, "echo")
		}
		if got, ok := m.Data.(string); !ok || got != "pong" {
			t.Errorf("received Data = %v, want %q", m.Data, "pong")
		}
	case <-ctx.Done():
		t.Fatalf("publisher did not receive its own publish via NOTIFY round-trip before deadline (%v)", ctx.Err())
	}
}

// TestIntegrationClusterFullMesh boots three ably-server instances
// against a single shared Postgres schema (i.e. a 3-node cluster),
// attaches one ably-go SDK client to each, then publishes one message
// via each WS in sequence. Every client must observe all three
// messages exactly once and in the same order — proving cross-node
// fan-out via LISTEN/NOTIFY plus the commit-order consistency that
// DESIGN.md §7.2 calls out.
func TestIntegrationClusterFullMesh(t *testing.T) {
	const nodes = 3

	pgc := pgtest.Start(t)
	dsn := pgc.FreshSchemaDSN(t)

	addrs := make([]string, nodes)
	for i := range nodes {
		addrs[i] = startServerOnDSN(t, dsn)
	}

	ctx, cancel := testCtx(t)
	defer cancel()

	receivers := make([]chan *ably.Message, nodes)
	clients := make([]*ably.Realtime, nodes)
	for i, addr := range addrs {
		clients[i] = newClient(t, addr)
		connect(t, clients[i])
		ch := clients[i].Channels.Get("mesh")
		recv := make(chan *ably.Message, nodes+1) // +1 to catch any duplicate without blocking
		receivers[i] = recv
		unsub, err := ch.SubscribeAll(ctx, func(m *ably.Message) {
			recv <- m
		})
		if err != nil {
			t.Fatalf("client %d SubscribeAll: %v", i, err)
		}
		defer unsub()
	}

	// Publish one message from each node, sequentially. Sequential
	// publishes keep the timestamp prefix of each channelSerial
	// monotonic across nodes, so the global commit order is the
	// publish order — which is what we assert all clients see.
	wantOrder := make([]string, nodes)
	for i := range nodes {
		name := nameFor(i)
		wantOrder[i] = name
		if err := clients[i].Channels.Get("mesh").Publish(ctx, name, dataFor(i)); err != nil {
			t.Fatalf("node %d Publish: %v", i, err)
		}
	}

	// Each receiver should see all N messages, exactly once each,
	// in the same order as the publish sequence.
	orders := make([][]string, nodes)
	for i, recv := range receivers {
		got := make([]string, 0, nodes)
		for range nodes {
			select {
			case m := <-recv:
				got = append(got, m.Name)
			case <-ctx.Done():
				t.Fatalf("client %d saw only %d/%d messages: %v", i, len(got), nodes, got)
			}
		}
		orders[i] = got

		// Drain-with-bounded-timeout: if any extra cm arrives in a
		// short window, that's a duplicate from the broker.
		select {
		case extra := <-recv:
			t.Errorf("client %d received unexpected extra message: %+v", i, extra)
		case <-time.After(200 * time.Millisecond):
		}
	}

	// Every name appears in every order, exactly once.
	for i, got := range orders {
		seen := make(map[string]int, nodes)
		for _, name := range got {
			seen[name]++
		}
		for _, want := range wantOrder {
			if seen[want] != 1 {
				t.Errorf("client %d saw %d copies of %q, want 1 (got=%v)", i, seen[want], want, got)
			}
		}
	}

	// Order consistency: every client should observe the cms in the
	// same order, matching the publish sequence (commit order on the
	// shared PG).
	for i, got := range orders {
		for j, name := range got {
			if name != wantOrder[j] {
				t.Errorf("client %d: order[%d] = %q, want %q (full order: %v)", i, j, name, wantOrder[j], got)
			}
		}
	}
}

// TestIntegrationClusterRESTPublishObservedAcrossNodes boots two
// servers on one shared schema; a WS subscriber on server B receives
// a publish issued via the REST endpoint on server A. Proves the
// cross-protocol cross-node path: REST → A's storage.Store → NOTIFY
// → B's LISTEN goroutine → B's Appender → B's Channel → B's WS
// attachment.
func TestIntegrationClusterRESTPublishObservedAcrossNodes(t *testing.T) {
	pgc := pgtest.Start(t)
	dsn := pgc.FreshSchemaDSN(t)

	addrA := startServerOnDSN(t, dsn)
	addrB := startServerOnDSN(t, dsn)

	ctx, cancel := testCtx(t)
	defer cancel()

	clientB := newClient(t, addrB)
	connect(t, clientB)
	chB := clientB.Channels.Get("cross")
	received := make(chan *ably.Message, 4)
	unsub, err := chB.SubscribeAll(ctx, func(m *ably.Message) {
		received <- m
	})
	if err != nil {
		t.Fatalf("client B SubscribeAll: %v", err)
	}
	defer unsub()

	postPublish(t, addrA, "cross", `{"name":"x","data":"hi"}`)

	select {
	case m := <-received:
		if m.Name != "x" {
			t.Errorf("Name = %q, want %q", m.Name, "x")
		}
		if got, ok := m.Data.(string); !ok || got != "hi" {
			t.Errorf("Data = %v, want %q", m.Data, "hi")
		}
	case <-ctx.Done():
		t.Fatalf("client B did not observe REST publish to server A before deadline (%v)", ctx.Err())
	}
}

func nameFor(i int) string { return "from-" + strconv.Itoa(i) }
func dataFor(i int) string { return "payload-" + strconv.Itoa(i) }

// TestIntegrationBatchPublishSDK publishes a multi-message batch via the
// SDK's PublishMultiple. A single frame is acked with count=1; reporting
// the inner-message count would over-ack and panic ably-go's pending
// emitter. All messages must round-trip to a subscriber.
func TestIntegrationBatchPublishSDK(t *testing.T) {
	addr := startServer(t)
	client := newClient(t, addr)
	connect(t, client)

	ctx, cancel := testCtx(t)
	defer cancel()

	ch := client.Channels.Get("batch")
	received := make(chan *ably.Message, 8)
	unsub, err := ch.SubscribeAll(ctx, func(m *ably.Message) { received <- m })
	if err != nil {
		t.Fatalf("SubscribeAll: %v", err)
	}
	defer unsub()

	// Must not error or panic the SDK connection goroutine.
	if err := ch.PublishMultiple(ctx, []*ably.Message{
		{Name: "a", Data: "1"}, {Name: "b", Data: "2"}, {Name: "c", Data: "3"},
	}); err != nil {
		t.Fatalf("PublishMultiple: %v", err)
	}

	got := map[string]bool{}
	for range 3 {
		select {
		case m := <-received:
			got[m.Name] = true
		case <-ctx.Done():
			t.Fatalf("only received %d/3 batch messages: %v", len(got), got)
		}
	}
	for _, n := range []string{"a", "b", "c"} {
		if !got[n] {
			t.Errorf("missing batch message %q (got %v)", n, got)
		}
	}
}
