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
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/realtime"
	"github.com/ably/ably-server/internal/rest"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/bbolt"
	"github.com/ably/ably-server/internal/storage/memory"
)

// Options configures an embedded ably-server. Only APIKey is required.
type Options struct {
	// APIKey is the credential in appId.keyId:keySecret form, e.g.
	// "app.key:secret". SDKs and REST clients present it back.
	APIKey string

	// Mode selects the storage backend: "memory" (default, ephemeral) or
	// "disk" (bbolt file under DataDir — durable across restarts, single
	// node). Cluster mode (shared Postgres) is intentionally not supported
	// in-process: it needs a DSN and would pull the Postgres driver into the
	// host binary. Run a child-process track or the standalone binary with
	// --mode cluster for shared-state scale.
	Mode string

	// DataDir is the directory for disk mode's bbolt file (created if
	// absent). Required when Mode is "disk".
	DataDir string

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

// New builds an in-process ably-server and returns it mounted as an
// http.Handler. Storage is memory mode by default, or disk (bbolt) when
// Options.Mode is "disk" — disk mode survives a host restart, which is what
// makes single-node durable sessions possible without a separate service.
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

	store, err := openStore(opts.Mode, opts.DataDir)
	if err != nil {
		return nil, err
	}
	manager := core.NewManager(store)
	rt := realtime.NewServer(key, manager, hb, logger)
	rs := rest.NewServer(key, manager, logger)

	return &Embedded{Handler: newMux(rt, rs), store: store}, nil
}

// Close releases the embedded server's storage.
func (e *Embedded) Close() error {
	return e.store.Close()
}

// openStore builds the storage backend for the embed: memory (default,
// ephemeral) or disk (bbolt file, durable across restarts). Cluster mode is
// deliberately unsupported in-process (it needs a DSN and pulls in the
// Postgres driver) — use a child-process track or the standalone binary.
func openStore(mode, dataDir string) (storage.Storage, error) {
	switch mode {
	case "", "memory":
		return memory.New(memory.Options{}), nil
	case "disk":
		if dataDir == "" {
			return nil, fmt.Errorf(`ablyembed: DataDir is required when Mode is "disk"`)
		}
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			return nil, fmt.Errorf("ablyembed: create DataDir %q: %w", dataDir, err)
		}
		return bbolt.Open(bbolt.Options{Path: filepath.Join(dataDir, "ably.db")})
	case "cluster":
		return nil, fmt.Errorf("ablyembed: cluster mode is not supported in-process; run a child-process track or the standalone binary with --mode cluster --db-dsn")
	default:
		return nil, fmt.Errorf("ablyembed: unknown Mode %q (valid: memory, disk)", mode)
	}
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
