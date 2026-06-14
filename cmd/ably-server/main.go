// Command ably-server is the open-source Ably-compatible server.
//
// At this stage it terminates WebSocket connections at `/`, sends a
// CONNECTED frame on connect, and emits periodic HEARTBEAT frames.
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
	apiKeyEnv = "ABLY_SERVER_API_KEY"
	dbDSNEnv  = "ABLY_SERVER_DB_DSN"
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
	if err := fs.Parse(opts.Args); err != nil {
		return 2
	}

	logger := newLogger(*logLevel, opts.Out)

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

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", rt.HandleWebSocket)
	mux.HandleFunc("POST /channels/{name}/messages", rs.HandlePublish)
	mux.HandleFunc("GET /channels/{name}/messages", rs.HandleHistory)
	mux.HandleFunc("PATCH /channels/{name}/messages/{serial}", rs.HandleMutate)
	mux.HandleFunc("GET /channels/{name}/messages/{serial}", rs.HandleMessage)
	mux.HandleFunc("GET /channels/{name}/messages/{serial}/versions", rs.HandleMessageVersions)
	mux.HandleFunc("GET /channels/{name}/presence", rs.HandlePresence)
	mux.HandleFunc("GET /channels/{name}/presence/history", rs.HandlePresenceHistory)
	mux.HandleFunc("GET /time", rs.HandleTime)
	mux.HandleFunc("GET /healthz", rs.HandleHealthz)
	mux.HandleFunc("GET /readyz", rs.HandleReadyz)

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

	<-ctx.Done()
	logger.Info("shutdown signal received")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownGrace)
	defer cancel()
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

func newLogger(level string, w io.Writer) *slog.Logger {
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
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: lvl}))
}
