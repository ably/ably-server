package main

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/realtime"
	"github.com/ably/ably-server/internal/rest"
	"github.com/ably/ably-server/internal/storage/memory"
)

// TestMuxWebSocketOnlyAtRoot verifies the WebSocket endpoint is bound to
// the exact root and does not act as a catch-all for unmatched GET paths.
func TestMuxWebSocketOnlyAtRoot(t *testing.T) {
	key, err := auth.ParseAPIKey("app.key:secret")
	if err != nil {
		t.Fatal(err)
	}
	mgr := core.NewManager(memory.New(memory.Options{}))
	logger := slog.New(slog.DiscardHandler)
	rt := realtime.NewServer(key, mgr, time.Hour, logger)
	rs := rest.NewServer(key, mgr, logger, nil)
	srv := httptest.NewServer(newMux(rt, rs))
	t.Cleanup(srv.Close)

	get := func(path string) *http.Response {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		req.SetBasicAuth("app.key", "secret")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		return resp
	}

	// An unknown path is not swallowed by the WS handler — it 404s.
	if resp := get("/stats"); resp.StatusCode != http.StatusNotFound {
		resp.Body.Close()
		t.Errorf("GET /stats = %d, want 404", resp.StatusCode)
	} else {
		resp.Body.Close()
	}

	// A real REST route still resolves.
	if resp := get("/time"); resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Errorf("GET /time = %d, want 200", resp.StatusCode)
	} else {
		resp.Body.Close()
	}

	// The root still routes to the WS upgrader: a non-upgrade GET there is
	// rejected by the upgrader with 400 and carries WS handshake headers,
	// which distinguishes "reached the upgrader" from a plain 404.
	resp := get("/")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("GET / (no upgrade) = %d, want 400 from the upgrader", resp.StatusCode)
	}
	if resp.Header.Get("Sec-Websocket-Version") == "" {
		t.Error("GET / should reach the WS upgrader (missing Sec-Websocket-Version header)")
	}
}
