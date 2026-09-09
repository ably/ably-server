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

	"github.com/ably/ably-server/internal/integration"
	"github.com/ably/ably-server/internal/server"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

const integrationAPIKey = "app.key:secret"

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

// TestIntegrationClusterMessageFanout boots three ably-server instances
// against a single shared Postgres schema (i.e. a 3-node cluster),
// attaches one ably-go SDK client to each, then publishes one message
// from each node in sequence. Every client must observe all three
// messages exactly once and in the same order — proving cross-node
// fan-out via LISTEN/NOTIFY plus the commit-order consistency that
// DESIGN.md §7.2 calls out.
func TestIntegrationClusterMessageFanout(t *testing.T) {
	integration.Require(t)
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
	//
	// The first goes over REST rather than its node's WebSocket. A REST
	// publish reaches the other nodes by the same NOTIFY round trip as a
	// realtime one (DESIGN.md §7.2), so this covers the route in with no
	// publishing connection behind it.
	wantOrder := make([]string, nodes)
	for i := range nodes {
		name := nameFor(i)
		wantOrder[i] = name
		if i == 0 {
			postPublish(t, addrs[i], "mesh",
				fmt.Sprintf(`{"name":%q,"data":%q}`, name, dataFor(i)))
			continue
		}
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

func nameFor(i int) string { return "from-" + strconv.Itoa(i) }
func dataFor(i int) string { return "payload-" + strconv.Itoa(i) }
