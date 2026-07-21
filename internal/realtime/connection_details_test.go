package realtime

import (
	"strings"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/protocol"
)

// TestConnectedCarriesConnectionDetails: the CONNECTED frame advertises a
// ConnectionDetails with the resolved clientId, a connectionKey that
// authenticates the connectionId (not just equal to it), the advertised
// limits, and maxIdleInterval aligned to the server heartbeat cadence
// (DESIGN.md §2.1, §8).
func TestConnectedCarriesConnectionDetails(t *testing.T) {
	const hb = 7 * time.Second
	srv, _ := newTestServer(t, hb)

	// A Basic-key connection with a concrete clientId resolves to that
	// clientId (§3.2).
	ws := dialClientID(t, srv, "alice")
	connected := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if connected.Action != protocol.ActionConnected {
		t.Fatalf("first frame = %v, want CONNECTED", connected.Action)
	}

	cd := connected.ConnectionDetails
	if cd == nil {
		t.Fatal("CONNECTED carried no connectionDetails")
	}
	if cd.ClientID != "alice" {
		t.Errorf("clientId = %q, want alice", cd.ClientID)
	}
	// connectionKey is connectionId plus an HMAC suffix authenticating the
	// pairing (DESIGN.md §8), not the bare connectionId.
	if !strings.HasPrefix(cd.ConnectionKey, connected.ConnectionID) {
		t.Errorf("connectionKey = %q, want it to start with connectionId %q", cd.ConnectionKey, connected.ConnectionID)
	}
	if cd.ConnectionKey == connected.ConnectionID {
		t.Errorf("connectionKey = %q, want more than the bare connectionId", cd.ConnectionKey)
	}
	if cd.MaxMessageSize != 65536 {
		t.Errorf("maxMessageSize = %d, want 65536", cd.MaxMessageSize)
	}
	if cd.MaxFrameSize != 524288 {
		t.Errorf("maxFrameSize = %d, want 524288", cd.MaxFrameSize)
	}
	if cd.MaxInboundRate != 1000 {
		t.Errorf("maxInboundRate = %d, want 1000", cd.MaxInboundRate)
	}
	if cd.ConnectionStateTTLMs != 120000 {
		t.Errorf("connectionStateTtl = %d ms, want 120000", cd.ConnectionStateTTLMs)
	}
	if cd.MaxIdleIntervalMs != hb.Milliseconds() {
		t.Errorf("maxIdleInterval = %d ms, want %d (heartbeat cadence)", cd.MaxIdleIntervalMs, hb.Milliseconds())
	}
}

// TestConnectedWildcardClientID: a Basic-key connection with no clientId
// param resolves to the wildcard "*", which is surfaced verbatim in
// connectionDetails.clientId (§3.2).
func TestConnectedWildcardClientID(t *testing.T) {
	srv, _ := newTestServer(t, time.Hour)

	ws := dial(t, srv, "")
	connected := readFrame(t, ws, protocol.FormatJSON, 2*time.Second)
	if connected.Action != protocol.ActionConnected {
		t.Fatalf("first frame = %v, want CONNECTED", connected.Action)
	}
	if connected.ConnectionDetails == nil {
		t.Fatal("CONNECTED carried no connectionDetails")
	}
	if got := connected.ConnectionDetails.ClientID; got != wildcardClientID {
		t.Errorf("clientId = %q, want wildcard %q", got, wildcardClientID)
	}
}
