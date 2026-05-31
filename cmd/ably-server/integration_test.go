//go:build integration

package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ably/ably-go/ably"

	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

const integrationAPIKey = "app.key:secret"

// startServer boots ably-server in cluster mode against a fresh
// Postgres schema, on a free TCP port discovered via the Ready hook
// in runOpts. It returns the bound "host:port" string; the server is
// torn down on t.Cleanup.
func startServer(t *testing.T) string {
	t.Helper()

	pgc := pgtest.Start(t)
	dsn := pgc.FreshSchemaDSN(t)

	ctx, cancel := context.WithCancel(context.Background())

	ready := make(chan net.Addr, 1)
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, runOpts{
			Args: []string{
				"--api-key=" + integrationAPIKey,
				"--mode=cluster",
				"--db-dsn=" + dsn,
				"--listen=127.0.0.1:0",
				"--log-level=error",
			},
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

// newClient builds an ably-go realtime client pointed at the running
// server. The client is closed on t.Cleanup.
func newClient(t *testing.T, addr string) *ably.Realtime {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split host:port %q: %v", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("port %q: %v", portStr, err)
	}
	client, err := ably.NewRealtime(
		ably.WithKey(integrationAPIKey),
		ably.WithEndpoint(host),
		ably.WithPort(port),
		ably.WithTLS(false),
		ably.WithInsecureAllowBasicAuthWithoutTLS(),
		ably.WithUseTokenAuth(false),
		ably.WithAutoConnect(false),
		ably.WithRealtimeRequestTimeout(5*time.Second),
		ably.WithLogLevel(ably.LogNone),
	)
	if err != nil {
		t.Fatalf("NewRealtime: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
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
