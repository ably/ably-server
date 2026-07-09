// Command ably-server is the open-source Ably-compatible server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/realtime"
	"github.com/ably/ably-server/internal/rest"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/bbolt"
	"github.com/ably/ably-server/internal/storage/memory"
	"github.com/ably/ably-server/internal/storage/postgres"
)

const (
	apiKeyEnv      = "ABLY_SERVER_API_KEY"
	dbDSNEnv       = "ABLY_SERVER_DB_DSN"
	logFormatEnv   = "ABLY_SERVER_LOG_FORMAT"
	debugListenEnv = "ABLY_SERVER_DEBUG_LISTEN"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, runOpts{
		Args:   os.Args[1:],
		Getenv: os.Getenv,
		Out:    os.Stdout,
	}))
}

// runOpts bundles run's inputs so the production main() and tests
// share one entry point. Args/Getenv/Out are required; Ready is an
// optional testing hook (see field doc).
type runOpts struct {
	// Args is the slice of CLI args (excluding os.Args[0]).
	Args []string

	// Getenv resolves an environment variable; tests pass a stub.
	Getenv func(string) string

	// Out is the writer used for logs and flag-parsing errors.
	Out io.Writer

	// Ready, when non-nil, receives the bound listener's address once
	// net.Listen returns — used by tests that pass --listen=:0 to
	// discover the ephemeral port. The send is bounded by ctx so a
	// missing receiver does not deadlock startup.
	Ready chan<- net.Addr

	// DebugReady, when non-nil, receives the bound debug listener's
	// address once it starts — used by tests that pass
	// --debug-listen=:0 to discover the ephemeral port. Only sent to
	// when --debug-listen is set; the send is bounded by ctx.
	DebugReady chan<- net.Addr
}

// run executes the server and returns the process exit code. All
// inputs are passed via runOpts so the function is testable without
// touching package-level state.
func run(ctx context.Context, opts runOpts) int {
	fs := flag.NewFlagSet("ably-server", flag.ContinueOnError)
	fs.SetOutput(opts.Out)
	listen := fs.String("listen", ":8080", "address for HTTP/WS listener")
	apiKey := fs.String("api-key", opts.Getenv(apiKeyEnv), "API key in appId.keyId:keySecret format (env: "+apiKeyEnv+")")
	mode := fs.String("mode", "memory", "storage backend: memory, disk, or cluster")
	dataDir := fs.String("data-dir", "./data", "data directory for disk mode (holds the bbolt file)")
	dbDSN := fs.String("db-dsn", opts.Getenv(dbDSNEnv), "libpq DSN for cluster mode, e.g. postgres://user:pw@host:5432/db?sslmode=disable (env: "+dbDSNEnv+")")
	hbInterval := fs.Duration("heartbeat-interval", realtime.DefaultHeartbeatInterval, "server-driven HEARTBEAT cadence")
	shutdownGrace := fs.Duration("shutdown-grace", 10*time.Second, "window to disconnect existing connections on SIGTERM")
	logLevel := fs.String("log-level", "info", "log level: debug, info, warn, error")
	logFormat := fs.String("log-format", envOr(opts.Getenv, logFormatEnv, "text"), "log format: text or json (env: "+logFormatEnv+")")
	debugListen := fs.String("debug-listen", opts.Getenv(debugListenEnv), "address for the pprof debug listener; disabled if empty (env: "+debugListenEnv+")")
	if err := fs.Parse(opts.Args); err != nil {
		return 2
	}

	logger, err := newLogger(*logLevel, *logFormat, opts.Out)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}

	if *apiKey == "" {
		logger.Error("api key is required", "flag", "--api-key", "env", apiKeyEnv)
		return 1
	}
	parsedKey, err := auth.ParseAPIKey(*apiKey)
	if err != nil {
		logger.Error("invalid api key", "err", err)
		return 1
	}

	store, err := openStorage(ctx, *mode, *dataDir, *dbDSN)
	if err != nil {
		logger.Error("open storage", "mode", *mode, "err", err)
		return 1
	}
	// Deferred so it fires after the graceful-shutdown block below
	// (srv.Shutdown drains in-flight HTTP requests, then this defer
	// closes the LISTEN goroutine + the pool — TASK-22's
	// postgres.Storage.Close).
	defer func() {
		if err := store.Close(); err != nil {
			logger.Error("close storage", "err", err)
		}
	}()
	logger.Info("storage ready", "mode", *mode)

	manager := core.NewManager(store)
	rt := realtime.NewServer(parsedKey, manager, *hbInterval, logger)
	rs := rest.NewServer(parsedKey, manager, logger)

	mux := newMux(rt, rs)

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Error("failed to listen", "addr", *listen, "err", err)
		return 1
	}

	if opts.Ready != nil {
		select {
		case opts.Ready <- listener.Addr():
		case <-ctx.Done():
			_ = listener.Close()
			return 1
		}
	}

	go func() {
		logger.Info("listening", "addr", listener.Addr().String())
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serve error", "err", err)
		}
	}()

	// debugSrv is non-nil only when --debug-listen is set; it serves
	// net/http/pprof's handlers (registered on http.DefaultServeMux by
	// this file's blank import) on a separate address so pprof is
	// never reachable via the main listener.
	var debugSrv *http.Server
	if *debugListen != "" {
		debugListener, err := net.Listen("tcp", *debugListen)
		if err != nil {
			logger.Error("failed to listen on debug address", "addr", *debugListen, "err", err)
			return 1
		}
		if opts.DebugReady != nil {
			select {
			case opts.DebugReady <- debugListener.Addr():
			case <-ctx.Done():
				_ = debugListener.Close()
				return 1
			}
		}
		debugSrv = &http.Server{ReadHeaderTimeout: 10 * time.Second}
		go func() {
			logger.Info("debug listening", "addr", debugListener.Addr().String())
			if err := debugSrv.Serve(debugListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("debug serve error", "err", err)
			}
		}()
	}

	<-ctx.Done()
	logger.Info("shutdown signal received")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownGrace)
	defer cancel()
	if debugSrv != nil {
		if err := debugSrv.Shutdown(shutdownCtx); err != nil {
			logger.Error("debug shutdown error", "err", err)
		}
	}
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown error", "err", err)
		return 1
	}
	return 0
}

// openStorage constructs the storage.Storage selected by mode:
//
//   - memory: in-process, no persistence.
//   - disk:   bbolt at <dataDir>/ably.db (dataDir created if absent).
//   - cluster: postgres at dbDSN (auto-migrates schema on Open;
//     spawns the LISTEN/NOTIFY broker — see DESIGN.md §7.2).
//
// ctx bounds the cluster-mode dial + ping + migrate; it's ignored by
// the in-process modes.
// newMux builds the HTTP routing table. The WebSocket endpoint is bound to
// the exact root with the `{$}` anchor: a bare `GET /` is a catch-all in
// Go 1.22's ServeMux and would feed every unmatched GET path to the
// upgrader (returning a confusing 400 with WebSocket headers). With `{$}`,
// only `/` upgrades and unknown paths fall through to a clean 404.
func newMux(rt *realtime.Server, rs *rest.Server) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", rt.HandleWebSocket)
	mux.HandleFunc("POST /channels/{name}/messages", rs.HandlePublish)
	mux.HandleFunc("GET /channels/{name}/messages", rs.HandleHistory)
	// ably-go's REST History() requests /history (TASK-57); serve it as
	// an alias so the SDK's history reads work.
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

func openStorage(ctx context.Context, mode, dataDir, dbDSN string) (storage.Storage, error) {
	switch mode {
	case "memory":
		return memory.New(memory.Options{}), nil
	case "disk":
		if dataDir == "" {
			return nil, errors.New("--data-dir is required when --mode=disk")
		}
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			return nil, fmt.Errorf("create data-dir %q: %w", dataDir, err)
		}
		return bbolt.Open(bbolt.Options{Path: filepath.Join(dataDir, "ably.db")})
	case "cluster":
		if dbDSN == "" {
			return nil, fmt.Errorf("--db-dsn is required when --mode=cluster (env: %s)", dbDSNEnv)
		}
		return postgres.Open(ctx, postgres.Options{DSN: dbDSN})
	default:
		return nil, fmt.Errorf("unknown --mode %q (valid: memory, disk, cluster)", mode)
	}
}

// newLogger builds the process logger. format selects the slog
// handler: "text" (the default) or "json"; any other value is a
// startup error.
func newLogger(level, format string, w io.Writer) (*slog.Logger, error) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	switch format {
	case "text":
		return slog.New(slog.NewTextHandler(w, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(w, opts)), nil
	default:
		return nil, fmt.Errorf("unknown --log-format %q (valid: text, json)", format)
	}
}

// envOr returns getenv(key) if non-empty, otherwise fallback. Used to
// seed a flag's default from its ABLY_SERVER_* env equivalent before
// flag.Parse runs, so --flag=... > env > this default all resolve
// correctly from a single fs.String call.
func envOr(getenv func(string) string, key, fallback string) string {
	if v := getenv(key); v != "" {
		return v
	}
	return fallback
}
