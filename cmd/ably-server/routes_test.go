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
	rt := realtime.NewServer([]auth.APIKey{key}, mgr, time.Hour, logger, nil, nil)
	rs := rest.NewServer([]auth.APIKey{key}, mgr, logger, nil, nil, nil)
	srv := httptest.NewServer(newMux(rt, rs, nil))
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
	if resp := get("/nonexistent"); resp.StatusCode != http.StatusNotFound {
		resp.Body.Close()
		t.Errorf("GET /nonexistent = %d, want 404", resp.StatusCode)
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

	// An unknown path carries the Ably ErrorInfo shape (code 40400) and the
	// X-Ably-Errorcode header the SDK reads — not a bare 404 (TASK-77).
	if resp := get("/nonexistent"); resp.Header.Get("X-Ably-Errorcode") != "40400" {
		resp.Body.Close()
		t.Errorf("GET /nonexistent X-Ably-Errorcode = %q, want 40400", resp.Header.Get("X-Ably-Errorcode"))
	} else {
		resp.Body.Close()
	}

	// A known path under a non-registered method (GET on the POST-only
	// requestToken) becomes a 40400 404, not Go's ServeMux 405 (TASK-77).
	if resp := get("/keys/app.key/requestToken"); resp.StatusCode != http.StatusNotFound ||
		resp.Header.Get("X-Ably-Errorcode") != "40400" {
		resp.Body.Close()
		t.Errorf("GET requestToken = %d / errcode %q, want 404 / 40400",
			resp.StatusCode, resp.Header.Get("X-Ably-Errorcode"))
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
