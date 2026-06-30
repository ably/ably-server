// Package ablyembed mounts an ably-server in-process as an http.Handler,
// the "ceiling" (best-case DX/overhead) track of the embedding PoC
// (EMBEDDING-POC.md §5, §8 M1). A Go host app imports this package, calls
// New, and serves the returned handler on its own listener — no child
// process, no reverse proxy, no FFI; realtime traffic still flows over a
// real local socket because that is how the host serves the handler.
//
// It reuses ably-server's own building blocks (internal/core, realtime,
// rest, storage/memory) exactly as cmd/ably-server/main.go wires them, so
// the embedded server is the same server, not a re-implementation. No
// change to ably-server is required — this package only composes it.
package ablyembed

import (
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/realtime"
	"github.com/ably/ably-server/internal/rest"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/memory"
)

// Options configures an embedded ably-server. Only APIKey is required.
type Options struct {
	// APIKey is the credential in appId.keyId:keySecret form, e.g.
	// "app.key:secret". SDKs and REST clients present it back.
	APIKey string

	// HeartbeatInterval is the server-driven HEARTBEAT cadence. Zero uses
	// realtime.DefaultHeartbeatInterval.
	HeartbeatInterval time.Duration

	// Logger receives the server's structured logs. Nil discards them.
	Logger *slog.Logger
}

// Embedded is a mounted in-process ably-server. Handler is ready to serve
// on any net/http listener; Close releases the backing storage and must
// be called on host-app shutdown.
type Embedded struct {
	// Handler serves the full Ably surface (WebSocket at "/", REST under
	// /channels, /keys, /time, /healthz, /readyz). Mount it on a dedicated
	// port (EMBEDDING-POC.md §6) and point an unmodified SDK at host+port.
	Handler http.Handler

	store storage.Storage
}

// New builds an in-process ably-server in memory mode (the PoC's
// zero-dependency mode) and returns it mounted as an http.Handler.
func New(opts Options) (*Embedded, error) {
	key, err := auth.ParseAPIKey(opts.APIKey)
	if err != nil {
		return nil, err
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	hb := opts.HeartbeatInterval
	if hb == 0 {
		hb = realtime.DefaultHeartbeatInterval
	}

	store := memory.New(memory.Options{})
	manager := core.NewManager(store)
	rt := realtime.NewServer(key, manager, hb, logger)
	rs := rest.NewServer(key, manager, logger)

	return &Embedded{Handler: newMux(rt, rs), store: store}, nil
}

// Close releases the embedded server's storage.
func (e *Embedded) Close() error {
	return e.store.Close()
}

// newMux mirrors cmd/ably-server/main.go's routing table so the embedded
// server exposes exactly the same endpoints as the standalone binary. The
// WebSocket endpoint is bound to the exact root with the {$} anchor so a
// bare GET / upgrades and unknown paths fall through to a clean 404.
func newMux(rt *realtime.Server, rs *rest.Server) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", rt.HandleWebSocket)
	mux.HandleFunc("POST /channels/{name}/messages", rs.HandlePublish)
	mux.HandleFunc("GET /channels/{name}/messages", rs.HandleHistory)
	mux.HandleFunc("GET /channels/{name}/history", rs.HandleHistory)
	mux.HandleFunc("PATCH /channels/{name}/messages/{serial}", rs.HandleMutate)
	mux.HandleFunc("GET /channels/{name}/messages/{serial}", rs.HandleMessage)
	mux.HandleFunc("GET /channels/{name}/messages/{serial}/versions", rs.HandleMessageVersions)
	mux.HandleFunc("GET /channels/{name}/presence", rs.HandlePresence)
	mux.HandleFunc("GET /channels/{name}/presence/history", rs.HandlePresenceHistory)
	mux.HandleFunc("POST /keys/{keyName}/requestToken", rs.HandleRequestToken)
	mux.HandleFunc("GET /time", rs.HandleTime)
	mux.HandleFunc("GET /healthz", rs.HandleHealthz)
	mux.HandleFunc("GET /readyz", rs.HandleReadyz)
	return mux
}
