package realtime

import (
	"context"
	"net"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ably/ably-go/ably"
)

// realtimeAddr extracts the host and port from an httptest server URL
// so the ably-go SDK can be pointed at it.
func realtimeAddr(t *testing.T, srvURL string) (string, int) {
	t.Helper()
	u, err := url.Parse(srvURL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatalf("split host:port: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("port: %v", err)
	}
	return host, port
}

// newSDKClient builds an ably-go realtime client wired at the
// httptest server and returns it without yet connecting.
func newSDKClient(t *testing.T, srv *httptest.Server) *ably.Realtime {
	t.Helper()
	host, port := realtimeAddr(t, srv.URL)
	client, err := ably.NewRealtime(
		ably.WithKey(testKey),
		ably.WithEndpoint(host),
		ably.WithPort(port),
		ably.WithTLS(false),
		ably.WithInsecureAllowBasicAuthWithoutTLS(),
		ably.WithUseTokenAuth(false),
		ably.WithAutoConnect(false),
		ably.WithRealtimeRequestTimeout(3*time.Second),
		ably.WithLogLevel(ably.LogNone),
	)
	if err != nil {
		t.Fatalf("NewRealtime: %v", err)
	}
	return client
}

// connectSDKClient drives the client to CONNECTED and fails the test
// on timeout.
func connectSDKClient(t *testing.T, client *ably.Realtime) {
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

// TestSDKConnectsAndCloses is a smoke test for SDK compatibility: the
// ably-go realtime client should reach CONNECTED against ably-server
// and then transition cleanly to CLOSED on Close().
func TestSDKConnectsAndCloses(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	client := newSDKClient(t, srv)

	closed := make(chan struct{}, 1)
	client.Connection.Once(ably.ConnectionEventClosed, func(ably.ConnectionStateChange) {
		closed <- struct{}{}
	})

	connectSDKClient(t, client)

	client.Close()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for CLOSED; current state: %v", client.Connection.State())
	}
}

// TestSDKPublishAndSubscribe drives publish/subscribe through the SDK:
// after attach a Publish should ACK successfully and the message
// should arrive at a Subscribe handler on the same connection (echo).
func TestSDKPublishAndSubscribe(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	client := newSDKClient(t, srv)
	t.Cleanup(func() { client.Close() })

	connectSDKClient(t, client)

	ch := client.Channels.Get("foo")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := ch.Attach(ctx); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if state := ch.State(); state != ably.ChannelStateAttached {
		t.Fatalf("post-Attach state = %v, want ATTACHED", state)
	}

	received := make(chan *ably.Message, 1)
	unsubscribe, err := ch.Subscribe(ctx, "greet", func(msg *ably.Message) {
		received <- msg
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer unsubscribe()

	if err := ch.Publish(ctx, "greet", "hello"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-received:
		if msg.Name != "greet" {
			t.Errorf("msg.Name = %q, want %q", msg.Name, "greet")
		}
		if msg.Data != "hello" {
			t.Errorf("msg.Data = %v (%T), want %q", msg.Data, msg.Data, "hello")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for subscribed message")
	}
}

// TestSDKAttachAndDetach drives the channel lifecycle through the SDK:
// Attach should take the channel to ATTACHED and Detach should take it
// to DETACHED.
func TestSDKAttachAndDetach(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
	client := newSDKClient(t, srv)
	t.Cleanup(func() { client.Close() })

	connectSDKClient(t, client)

	ch := client.Channels.Get("foo")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := ch.Attach(ctx); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if state := ch.State(); state != ably.ChannelStateAttached {
		t.Errorf("post-Attach state = %v, want ATTACHED", state)
	}

	if err := ch.Detach(ctx); err != nil {
		t.Fatalf("Detach: %v", err)
	}
	if state := ch.State(); state != ably.ChannelStateDetached {
		t.Errorf("post-Detach state = %v, want DETACHED", state)
	}
}
