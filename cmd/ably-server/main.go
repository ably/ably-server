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
	"strings"
	"syscall"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/config"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/metrics"
	"github.com/ably/ably-server/internal/realtime"
	"github.com/ably/ably-server/internal/rest"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/bbolt"
	"github.com/ably/ably-server/internal/storage/memory"
	"github.com/ably/ably-server/internal/storage/postgres"
)

const (
	apiKeyEnv        = "ABLY_SERVER_API_KEY"
	dbDSNEnv         = "ABLY_SERVER_DB_DSN"
	logFormatEnv     = "ABLY_SERVER_LOG_FORMAT"
	debugListenEnv   = "ABLY_SERVER_DEBUG_LISTEN"
	modeEnv          = "ABLY_SERVER_MODE"
	listenEnv        = "ABLY_SERVER_LISTEN"
	dataDirEnv       = "ABLY_SERVER_DATA_DIR"
	shutdownGraceEnv = "ABLY_SERVER_SHUTDOWN_GRACE"
	logLevelEnv      = "ABLY_SERVER_LOG_LEVEL"
	configPathEnv    = "ABLY_SERVER_CONFIG"
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
	// The config file's path must be known before the flags it seeds
	// are defined below, so it's resolved by hand (flag > env) ahead
	// of the real flag.Parse pass. --config is still registered as a
	// flag further down purely so fs.Parse recognises it and --help
	// lists it; its value there is unused.
	configPath := opts.Getenv(configPathEnv)
	if p := config.PathFromArgs(opts.Args); p != "" {
		configPath = p
	}
	var file config.File
	if configPath != "" {
		f, err := config.Load(configPath)
		if err != nil {
			fmt.Fprintln(opts.Out, err)
			return 1
		}
		file = *f
	}

	shutdownGraceDefault, err := config.DefaultDuration(opts.Getenv(shutdownGraceEnv), file.ShutdownGrace, 10*time.Second)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}

	fs := flag.NewFlagSet("ably-server", flag.ContinueOnError)
	fs.SetOutput(opts.Out)
	fs.String("config", configPath, "path to an optional TOML config file (env: "+configPathEnv+")")
	listen := fs.String("listen", config.Default(opts.Getenv(listenEnv), file.Listen, ":8080"), "address for HTTP/WS listener (env: "+listenEnv+")")
	var apiKeyFlags multiFlag
	fs.Var(&apiKeyFlags, "api-key", "API key in appId.keyId:keySecret format; repeatable (env: "+apiKeyEnv+", comma-separated)")
	mode := fs.String("mode", config.Default(opts.Getenv(modeEnv), file.Mode, "memory"), "storage backend: memory, disk, or cluster (env: "+modeEnv+")")
	dataDir := fs.String("data-dir", config.Default(opts.Getenv(dataDirEnv), file.DataDir, "./data"), "data directory for disk mode (holds the bbolt file) (env: "+dataDirEnv+")")
	dbDSN := fs.String("db-dsn", config.Default(opts.Getenv(dbDSNEnv), file.DBDSN, ""), "libpq DSN for cluster mode, e.g. postgres://user:pw@host:5432/db?sslmode=disable (env: "+dbDSNEnv+")")
	hbInterval := fs.Duration("heartbeat-interval", realtime.DefaultHeartbeatInterval, "server-driven HEARTBEAT cadence")
	shutdownGrace := fs.Duration("shutdown-grace", shutdownGraceDefault, "window to disconnect existing connections on SIGTERM (env: "+shutdownGraceEnv+")")
	logLevel := fs.String("log-level", config.Default(opts.Getenv(logLevelEnv), file.LogLevel, "info"), "log level: debug, info, warn, error (env: "+logLevelEnv+")")
	logFormat := fs.String("log-format", config.Default(opts.Getenv(logFormatEnv), file.LogFormat, "text"), "log format: text or json (env: "+logFormatEnv+")")
	debugListen := fs.String("debug-listen", config.Default(opts.Getenv(debugListenEnv), file.DebugListen, ""), "address for the pprof debug listener; disabled if empty (env: "+debugListenEnv+")")
	if err := fs.Parse(opts.Args); err != nil {
		return 2
	}

	logger, err := newLogger(*logLevel, *logFormat, opts.Out)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}

	keySpecs := resolveAPIKeys([]string(apiKeyFlags), opts.Getenv(apiKeyEnv), file)
	if len(keySpecs) == 0 {
		logger.Error("at least one api key is required", "flag", "--api-key", "env", apiKeyEnv)
		return 1
	}
	parsedKeys := make([]auth.APIKey, 0, len(keySpecs))
	for _, spec := range keySpecs {
		k, err := auth.ParseAPIKey(spec)
		if err != nil {
			logger.Error("invalid api key", "err", err)
			return 1
		}
		parsedKeys = append(parsedKeys, k)
	}
	// All keys must belong to the same app: the server owns one channel
	// namespace, so keys spanning multiple appIds are a misconfiguration
	// (DESIGN.md §3).
	appID := parsedKeys[0].AppID
	for _, k := range parsedKeys[1:] {
		if k.AppID != appID {
			logger.Error("all api keys must share the same appId", "appId", appID, "conflicting", k.AppID)
			return 1
		}
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

	m := metrics.New()
	manager := core.NewManager(store)
	rt := realtime.NewServer(parsedKeys, manager, *hbInterval, logger, m)
	// ready is non-nil only for backends with an external dependency
	// worth probing (currently postgres.Storage); memory/disk leave it
	// nil and /readyz reports 200 unconditionally (TASK-62).
	ready, _ := store.(storage.Pinger)
	rs := rest.NewServer(parsedKeys, manager, logger, ready, m)

	mux := newMux(rt, rs, m)

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
	// srv.Shutdown stops accepting new connections and drains in-flight
	// HTTP handlers — but a WebSocket handler blocks in the connection
	// loop until its socket closes, so it would otherwise hang until the
	// deadline. Run it concurrently with rt.Shutdown, which sends
	// DISCONNECTED to every live WebSocket and paces the closures across
	// the grace window (DESIGN.md §11), unblocking those handlers.
	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.Shutdown(shutdownCtx) }()
	rt.Shutdown(shutdownCtx)
	if err := <-srvErr; err != nil {
		logger.Error("shutdown error", "err", err)
		return 1
	}
	return 0
}

// multiFlag collects a repeatable string flag (each --api-key occurrence
// appends one value), so multiple keys can be configured on the command
// line (DESIGN.md §3, §9).
type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }

func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

// resolveAPIKeys resolves the configured API key specs with precedence
// flag > env > file, applied as whole sets (DESIGN.md §3, §9): repeated
// --api-key flags win outright; otherwise a comma-separated
// ABLY_SERVER_API_KEY; otherwise the config file's api-keys array plus
// its singular api-key. Whitespace around each spec is trimmed and empty
// entries dropped.
func resolveAPIKeys(flagKeys []string, env string, file config.File) []string {
	if len(flagKeys) > 0 {
		return splitTrim(flagKeys)
	}
	if env != "" {
		return splitTrim(strings.Split(env, ","))
	}
	var fromFile []string
	fromFile = append(fromFile, file.APIKeys...)
	if file.APIKey != "" {
		fromFile = append(fromFile, file.APIKey)
	}
	return splitTrim(fromFile)
}

// splitTrim trims whitespace from each entry and drops empties.
func splitTrim(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
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
func newMux(rt *realtime.Server, rs *rest.Server, m *metrics.Metrics) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", rt.HandleWebSocket)

	// REST routes are wrapped so each records ably_http_requests_total by
	// route pattern / method / status (DESIGN.md §10). The WebSocket route
	// is excluded — its handler blocks for the connection's whole lifetime,
	// which the connection metrics already cover. /metrics itself is
	// unwrapped so scrapes don't inflate the counters.
	rest := func(pattern string, h http.HandlerFunc) {
		mux.Handle(pattern, instrumentHTTP(m, pattern, h))
	}
	rest("POST /channels/{name}/messages", rs.HandlePublish)
	rest("GET /channels/{name}/messages", rs.HandleHistory)
	// ably-go's REST History() requests /history (TASK-57); serve it as
	// an alias so the SDK's history reads work.
	rest("GET /channels/{name}/history", rs.HandleHistory)
	rest("PATCH /channels/{name}/messages/{serial}", rs.HandleMutate)
	rest("GET /channels/{name}/messages/{serial}", rs.HandleMessage)
	rest("GET /channels/{name}/messages/{serial}/versions", rs.HandleMessageVersions)
	rest("GET /channels/{name}/presence", rs.HandlePresence)
	rest("GET /channels/{name}/presence/history", rs.HandlePresenceHistory)
	rest("POST /keys/{keyName}/requestToken", rs.HandleRequestToken)
	rest("GET /time", rs.HandleTime)
	rest("GET /healthz", rs.HandleHealthz)
	rest("GET /readyz", rs.HandleReadyz)

	// /metrics is served unauthenticated on the main listener, like
	// /healthz (DESIGN.md §10). Skipped when no Metrics is configured
	// (only tests pass nil; production always wires one).
	if m != nil {
		mux.Handle("GET /metrics", m.Handler())
	}
	return mux
}

// instrumentHTTP wraps a REST handler so it records the served request
// against ably_http_requests_total, labelled by the route pattern (kept
// low-cardinality by using the pattern, not the concrete path), method,
// and response status.
func instrumentHTTP(m *metrics.Metrics, pattern string, h http.HandlerFunc) http.HandlerFunc {
	route := routePath(pattern)
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		h(rec, r)
		m.HTTPRequest(route, r.Method, rec.status)
	}
}

// routePath strips the leading "METHOD " from a ServeMux pattern, leaving
// the path template used as the metric's route label.
func routePath(pattern string) string {
	if i := strings.IndexByte(pattern, ' '); i >= 0 {
		return pattern[i+1:]
	}
	return pattern
}

// statusRecorder captures the response status code written by a handler
// so the HTTP metrics middleware can label by it. A handler that never
// calls WriteHeader leaves the default 200.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	s.wroteHeader = true
	return s.ResponseWriter.Write(b)
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
