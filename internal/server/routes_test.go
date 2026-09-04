package server

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/config"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
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
	logger := logging.New(slog.DiscardHandler)
	shared, err := newSharedProtocol(t.Context(), []auth.APIKey{key}, config.File{}, mgr, nil, logger, sharedOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shared.Close)
	rs := rest.NewServer([]auth.APIKey{key}, logger, nil)
	srv := httptest.NewServer(newMux(shared, rs, nil, false))
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
	// X-Ably-Errorcode header the SDK reads — not a bare 404.
	if resp := get("/nonexistent"); resp.Header.Get("X-Ably-Errorcode") != "40400" {
		resp.Body.Close()
		t.Errorf("GET /nonexistent X-Ably-Errorcode = %q, want 40400", resp.Header.Get("X-Ably-Errorcode"))
	} else {
		resp.Body.Close()
	}

	// A known path under a non-registered method (GET on the POST-only
	// requestToken) becomes a 40400 404, not Go's ServeMux 405.
	if resp := get("/keys/app.key/requestToken"); resp.StatusCode != http.StatusNotFound ||
		resp.Header.Get("X-Ably-Errorcode") != "40400" {
		resp.Body.Close()
		t.Errorf("GET requestToken = %d / errcode %q, want 404 / 40400",
			resp.StatusCode, resp.Header.Get("X-Ably-Errorcode"))
	} else {
		resp.Body.Close()
	}

	// The root routes to the websocket transport, which answers a request
	// that is not an upgrade with an Ably-shaped 404 naming the path. This
	// server's own upgrader used to answer 400 with handshake headers.
	resp := get("/")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET / (no upgrade) = %d, want 404", resp.StatusCode)
	}
}

// TestStatsStubGating pins the /stats stub gating: the /stats compatibility stub (a
// non-goal, DESIGN.md §1) is unregistered by default — GET/POST /stats
// fall through to the catch-all Ably-shaped 40400 — and only served when
// --enable-stats-stub is set.
func TestStatsStubGating(t *testing.T) {
	key, err := auth.ParseAPIKey("app.key:secret")
	if err != nil {
		t.Fatal(err)
	}
	newServer := func(enableStatsStub bool) *httptest.Server {
		mgr := core.NewManager(memory.New(memory.Options{}))
		logger := logging.New(slog.DiscardHandler)
		shared, err := newSharedProtocol(t.Context(), []auth.APIKey{key}, config.File{}, mgr, nil, logger, sharedOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(shared.Close)
		rs := rest.NewServer([]auth.APIKey{key}, logger, nil)
		return httptest.NewServer(newMux(shared, rs, nil, enableStatsStub))
	}

	get := func(srv *httptest.Server, path string) *http.Response {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		req.SetBasicAuth("app.key", "secret")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		return resp
	}

	t.Run("disabled by default", func(t *testing.T) {
		srv := newServer(false)
		t.Cleanup(srv.Close)

		resp := get(srv, "/stats")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET /stats = %d, want 404", resp.StatusCode)
		}
		if got := resp.Header.Get("X-Ably-Errorcode"); got != "40400" {
			t.Errorf("GET /stats X-Ably-Errorcode = %q, want 40400", got)
		}

		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/stats", nil)
		req.SetBasicAuth("app.key", "secret")
		postResp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST /stats: %v", err)
		}
		defer postResp.Body.Close()
		if postResp.StatusCode != http.StatusNotFound {
			t.Errorf("POST /stats = %d, want 404", postResp.StatusCode)
		}
	})

	t.Run("enabled serves the stub", func(t *testing.T) {
		srv := newServer(true)
		t.Cleanup(srv.Close)

		resp := get(srv, "/stats")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET /stats = %d, want 200", resp.StatusCode)
		}

		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/stats", nil)
		req.SetBasicAuth("app.key", "secret")
		postResp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST /stats: %v", err)
		}
		defer postResp.Body.Close()
		if postResp.StatusCode != http.StatusCreated {
			t.Errorf("POST /stats = %d, want 201", postResp.StatusCode)
		}
	})
}
