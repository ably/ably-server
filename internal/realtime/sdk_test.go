package realtime

import (
	"net"
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

// TestSDKConnectsAndCloses is a smoke test for SDK compatibility: the
// ably-go realtime client should reach CONNECTED against ably-server
// and then transition cleanly to CLOSED on Close().
func TestSDKConnectsAndCloses(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)
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

	connected := make(chan ably.ConnectionStateChange, 1)
	closed := make(chan ably.ConnectionStateChange, 1)
	client.Connection.Once(ably.ConnectionEventConnected, func(c ably.ConnectionStateChange) {
		connected <- c
	})
	client.Connection.Once(ably.ConnectionEventClosed, func(c ably.ConnectionStateChange) {
		closed <- c
	})

	client.Connect()
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for CONNECTED; current state: %v", client.Connection.State())
	}

	client.Close()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for CLOSED; current state: %v", client.Connection.State())
	}
}
