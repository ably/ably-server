// Package server assembles the ably-server process: flag/env/config
// parsing, storage/realtime/REST wiring, HTTP routing, and graceful
// shutdown. cmd/ably-server is a thin wrapper that calls Run; tests
// (including the SDK suites in internal/server/integrationtest) call
// Run directly so they can
// discover ephemeral listener addresses via Opts.Ready/DebugReady
// without shelling out to a built binary.
package server

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
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/config"
	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/fixtures"
	"github.com/ably/ably-server/internal/handles"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/metrics"
	"github.com/ably/ably-server/internal/rest"
	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/bbolt"
	"github.com/ably/ably-server/internal/storage/memory"
	"github.com/ably/ably-server/internal/storage/postgres"
	"github.com/ably/ably-server/internal/tracing"
	"github.com/ably/ably-server/internal/version"
)

const (
	keysEnv            = "ABLY_SERVER_KEYS"
	postgresDSNEnv     = "ABLY_SERVER_POSTGRES_DSN"
	logFormatEnv       = "ABLY_SERVER_LOG_FORMAT"
	debugListenEnv     = "ABLY_SERVER_DEBUG_LISTEN"
	modeEnv            = "ABLY_SERVER_MODE"
	listenEnv          = "ABLY_SERVER_LISTEN"
	dataDirEnv         = "ABLY_SERVER_DATA_DIR"
	shutdownGraceEnv   = "ABLY_SERVER_SHUTDOWN_GRACE"
	logLevelEnv        = "ABLY_SERVER_LOG_LEVEL"
	configPathEnv      = "ABLY_SERVER_CONFIG"
	addrFileEnv        = "ABLY_SERVER_ADDR_FILE"
	enableStatsStubEnv = "ABLY_SERVER_ENABLE_STATS_STUB"
	keysDirEnv         = "ABLY_SERVER_KEYS_DIR"
	namespacesDirEnv   = "ABLY_SERVER_NAMESPACES_DIR"
	appStatusFileEnv   = "ABLY_SERVER_APP_STATUS_FILE"
)

// Opts bundles Run's inputs so the production main() and tests
// share one entry point. Args/Getenv/Out are required; Ready is an
// optional testing hook (see field doc).
type Opts struct {
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

// Run executes the server and returns the process exit code. All
// inputs are passed via Opts so the function is testable without
// touching package-level state.
func Run(ctx context.Context, opts Opts) int {
	// --version is answered before anything else, so identifying a
	// binary never depends on its configuration being loadable.
	if hasVersionFlag(opts.Args) {
		fmt.Fprintln(opts.Out, version.String())
		return 0
	}

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
	enableStatsStubDefault, err := config.DefaultBool(opts.Getenv(enableStatsStubEnv), file.EnableStatsStub, false)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}

	fs := flag.NewFlagSet("ably-server", flag.ContinueOnError)
	fs.SetOutput(opts.Out)
	fs.String("config", configPath, "path to an optional TOML config file (env: "+configPathEnv+")")
	// Handled above, ahead of config loading; registered only so
	// fs.Parse accepts it and --help lists it.
	fs.Bool("version", false, "print the version, commit and toolchain, then exit")
	listen := fs.String("listen", config.Default(opts.Getenv(listenEnv), file.Listen, ":8080"), "address for HTTP/WS listener (env: "+listenEnv+")")
	var keysFlags multiFlag
	fs.Var(&keysFlags, "keys", "API key in appId.keyId:keySecret format; repeatable (env: "+keysEnv+", comma-separated)")
	mode := fs.String("mode", config.Default(opts.Getenv(modeEnv), file.Mode, "memory"), "storage backend: memory, disk, or cluster (env: "+modeEnv+")")
	dataDir := fs.String("data-dir", config.Default(opts.Getenv(dataDirEnv), file.DataDir, "./data"), "data directory for disk mode (holds the bbolt file) (env: "+dataDirEnv+")")
	postgresDSN := fs.String("postgres-dsn", config.Default(opts.Getenv(postgresDSNEnv), file.PostgresDSN, ""), "libpq DSN for cluster mode, e.g. postgres://user:pw@host:5432/db?sslmode=disable (env: "+postgresDSNEnv+")")
	hbInterval := fs.Duration("heartbeat-interval", 15*time.Second, "server-driven HEARTBEAT cadence")
	remainPresentFor := fs.Duration("presence-remain-for", 0, "how long a presence member survives an abrupt disconnect before its LEAVE is synthesised, so a resume+re-enter avoids a flicker (DESIGN.md §12.5)")
	shutdownGrace := fs.Duration("shutdown-grace", shutdownGraceDefault, "window to disconnect existing connections on SIGTERM (env: "+shutdownGraceEnv+")")
	logLevel := fs.String("log-level", config.Default(opts.Getenv(logLevelEnv), file.LogLevel, "info"), "log level: "+logging.LevelNames+" (env: "+logLevelEnv+")")
	logFormat := fs.String("log-format", config.Default(opts.Getenv(logFormatEnv), file.LogFormat, "text"), "log format: text or json (env: "+logFormatEnv+")")
	debugListen := fs.String("debug-listen", config.Default(opts.Getenv(debugListenEnv), file.DebugListen, ""), "address for the pprof debug listener; disabled if empty (env: "+debugListenEnv+")")
	addrFile := fs.String("addr-file", opts.Getenv(addrFileEnv), "path to write the bound listener address to once listening; used by a parent process to discover an ephemeral (--listen :0) port (env: "+addrFileEnv+")")
	enableStatsStub := fs.Bool("enable-stats-stub", enableStatsStubDefault, "register the GET/POST /stats compatibility stub used by SDK test flows; unregistered (404) by default (env: "+enableStatsStubEnv+")")
	keysDir := fs.String("keys-dir", config.Default(opts.Getenv(keysDirEnv), file.KeysDir, ""), "directory holding one API key per file, re-read while the server runs (env: "+keysDirEnv+")")
	namespacesDir := fs.String("namespaces-dir", config.Default(opts.Getenv(namespacesDirEnv), file.NamespacesDir, ""), "directory holding one namespace per file, re-read while the server runs (env: "+namespacesDirEnv+")")
	appStatusFile := fs.String("app-status-file", config.Default(opts.Getenv(appStatusFileEnv), file.AppStatusFile, ""), "file holding the app's status, re-read while the server runs; no file, or "+strconv.Quote(handles.StatusEnabled)+", means the app is served, and anything else disables it (env: "+appStatusFileEnv+")")
	if err := fs.Parse(opts.Args); err != nil {
		return 2
	}

	logger, err := newLogger(*logLevel, *logFormat, opts.Out)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}
	logger.Info("starting",
		"version", version.Version(),
		"commit", version.Commit(),
		"go", runtime.Version(),
	)

	// The watched sources are read before anything is built, because the keys
	// and namespaces they carry are as much this server's configuration as the
	// flags are — the app id is derived from the whole set, and the app is
	// built holding all of it (DESIGN.md §9.1). They are re-read from here on
	// by the reloader started below.
	sources := config.Dynamic{
		KeysDir:       *keysDir,
		NamespacesDir: *namespacesDir,
		AppStatusFile: *appStatusFile,
	}
	watched, err := sources.Read()
	if err != nil {
		logger.Error("reading the watched config", "err", err)
		return 1
	}

	// The keys directory is a key source in its own right, layered over
	// whatever the flags, the environment or the config file supplied. The
	// reloader below merges the two the same way on every reload; here it is
	// done once more, by hand, because the app id has to come out of the whole
	// set before there is an app to reconfigure.
	staticKeys := resolveAPIKeys([]string(keysFlags), opts.Getenv(keysEnv), file)
	keySpecs := make([]keySpec, 0, len(staticKeys)+len(watched.Keys))
	keySpecs = append(keySpecs, staticKeys...)
	for _, entry := range watched.Keys {
		keySpecs = append(keySpecs, keySpec{key: entry.Key, capability: entry.Capability})
	}
	if len(keySpecs) == 0 {
		logger.Error("at least one api key is required", "flag", "--keys", "env", keysEnv, "keysDirFlag", "--keys-dir")
		return 1
	}
	parsedKeys, appID, err := parseAPIKeys(keySpecs)
	if err != nil {
		logger.Error("invalid api key configuration", "err", err)
		return 1
	}

	// The namespaces the config file and the flags supplied are re-applied
	// unchanged for as long as this server runs, so they are all one version of
	// themselves, stamped now. A namespace from --namespaces-dir carries its
	// file's modification time instead, so editing that file is what makes the
	// namespace map see a new version of it.
	staticNamespaces := config.StampNamespaces(file.Namespaces, time.Now())

	namespaces := mergeNamespaces(staticNamespaces, watched.Namespaces)
	if err := config.ValidateNamespaces(namespaces); err != nil {
		logger.Error("invalid namespace configuration", "err", err)
		return 1
	}

	// OpenTelemetry tracing is off unless the standard OTEL_* env asks for
	// it (DESIGN.md §10). Setup uses a background context so a SIGTERM
	// cancelling ctx does not tear the exporter down before graceful
	// shutdown flushes it. When disabled this installs no exporter and
	// starts no goroutine.
	tp, err := tracing.Setup(context.Background(), opts.Getenv)
	if err != nil {
		logger.Error("setup tracing", "err", err)
		return 1
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown tracing", "err", err)
		}
	}()
	// The endpoints this server serves itself take no tracer: what is traced
	// is the channel surface, which is the shared module's and starts its own
	// spans on whatever tracer provider this installed. HTTP spans come from
	// the otelhttp handler below.
	if tp.Enabled {
		logger.Info("tracing enabled")
	}

	store, err := openStorage(ctx, *mode, *dataDir, *postgresDSN)
	if err != nil {
		logger.Error("open storage", "mode", *mode, "err", err)
		return 1
	}
	// Deferred so it fires after the graceful-shutdown block below
	// (srv.Shutdown drains in-flight HTTP requests, then this defer
	// closes the LISTEN goroutine + the pool via
	// postgres.Storage.Close).
	defer func() {
		if err := store.Close(); err != nil {
			logger.Error("close storage", "err", err)
		}
	}()
	logger.Info("storage ready", "mode", *mode)

	m := metrics.New()
	manager := core.NewManager(store)

	// Pre-seed presence fixtures declared in the config file before
	// serving traffic (DESIGN.md §9, §12.5). Malformed sections are a
	// startup error.
	spec, err := fixtureSpec(file)
	if err != nil {
		logger.Error("invalid config fixtures", "err", err)
		return 1
	}
	if spec != nil {
		if err := fixtures.Seed(ctx, manager, spec, logger); err != nil {
			logger.Error("seed fixtures", "err", err)
			return 1
		}
	}

	// The protocol is served from github.com/ably/server-protocol/go, which this
	// server supplies with its storage, its keys and its logger. What is left
	// here is what that module does not describe: this server's own token
	// endpoint, its liveness probes, and the stats stub SDK test flows want.
	shared, err := newSharedProtocol(ctx, parsedKeys, namespaces, manager, m, logger, sharedOptions{
		heartbeatInterval: *hbInterval,
		remainPresentFor:  *remainPresentFor,
	})
	if err != nil {
		logger.Error("wiring the shared protocol code", "err", err)
		return 1
	}

	// ready is non-nil only for backends with an external dependency
	// worth probing (currently postgres.Storage); memory/disk leave it
	// nil and /readyz reports 200 unconditionally.
	ready, _ := store.(storage.Pinger)
	rs := rest.NewServer(parsedKeys, logger, ready)

	// The watched sources are applied once more here, over the app that was
	// just built from them, so that everything a reload does goes through the
	// one code path — and so that an app-status file saying the app is
	// disabled is in force before the listener opens.
	reload := &reloader{
		sources:          sources,
		appID:            appID,
		staticKeys:       staticKeys,
		staticNamespaces: staticNamespaces,
		app:              shared.App(),
		rest:             rs,
		log:              logger,
	}
	if sources.Watched() {
		logger.Info("watching config",
			"keysDir", sources.KeysDir,
			"namespacesDir", sources.NamespacesDir,
			"appStatusFile", sources.AppStatusFile,
			"interval", reloadInterval,
		)
	}
	if err := reload.load(); err != nil {
		logger.Error("applying the watched config", "err", err)
		return 1
	}
	if sources.Watched() {
		go reload.run(ctx, reloadInterval)
	}

	mux := newMux(shared, rs, m, *enableStatsStub)

	// When tracing is enabled, otelhttp wraps the whole mux so every HTTP
	// request (including the REST handlers) gets a server span; the WS
	// upgrade request's span then spans the connection handler too. When
	// disabled the mux is served directly with no wrapping overhead.
	// Every request is tracked so it can be cancelled on shutdown; a
	// websocket handler otherwise runs until its client goes away.
	conns := newConnTracker()

	var handler http.Handler = conns.Handler(mux)
	if tp.Enabled {
		handler = otelhttp.NewHandler(mux, "http.server")
	}

	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Error("failed to listen", "addr", *listen, "err", err)
		return 1
	}

	// Publish the bound address so a parent process (the sandbox
	// provisioner, DESIGN.md §17) can discover the port a `--listen
	// 127.0.0.1:0` bind resolved to. Written atomically so a reader
	// polling the path never observes a partial address.
	if *addrFile != "" {
		if err := writeAddrFile(*addrFile, listener.Addr().String()); err != nil {
			logger.Error("failed to write addr-file", "path", *addrFile, "err", err)
			_ = listener.Close()
			return 1
		}
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
	// net/http/pprof's handlers plus /metrics (newDebugMux) on a separate
	// address so neither is ever reachable via the main listener.
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
		debugSrv = &http.Server{Handler: newDebugMux(m), ReadHeaderTimeout: 10 * time.Second}
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
	conns.CloseAll()
	shared.Close()
	if err := <-srvErr; err != nil {
		logger.Error("shutdown error", "err", err)
		return 1
	}
	return 0
}

// multiFlag collects a repeatable string flag (each --keys occurrence
// appends one value), so multiple keys can be configured on the command
// line (DESIGN.md §3, §9).
type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }

func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

// keySpec is one resolved API key: its Ably-format spec plus an optional
// per-key capability (an x-ably-capability-format JSON string, empty for
// full capability) — DESIGN.md §3.1, §9.
type keySpec struct {
	key        string
	capability string
}

// resolveAPIKeys resolves the configured API keys with precedence
// flag > env > file, applied as whole sets (DESIGN.md §3, §9): repeated
// --keys flags win outright; otherwise a comma-separated ABLY_SERVER_KEYS;
// otherwise the config file's structured [[keys]] entries. Flag/env keys
// are always full-capability; only [[keys]] entries may carry a narrowing
// capability. Whitespace around each spec is trimmed and empty entries
// dropped.
func resolveAPIKeys(flagKeys []string, env string, file config.File) []keySpec {
	if len(flagKeys) > 0 {
		return fullCapSpecs(splitTrim(flagKeys))
	}
	if env != "" {
		return fullCapSpecs(splitTrim(strings.Split(env, ",")))
	}
	specs := make([]keySpec, 0, len(file.Keys))
	for _, e := range file.Keys {
		if s := strings.TrimSpace(e.Key); s != "" {
			specs = append(specs, keySpec{key: s, capability: e.Capability})
		}
	}
	return specs
}

// fullCapSpecs wraps bare key strings as full-capability keySpecs.
func fullCapSpecs(keys []string) []keySpec {
	out := make([]keySpec, 0, len(keys))
	for _, k := range keys {
		out = append(out, keySpec{key: k})
	}
	return out
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

// parseAPIKeys parses the resolved key specs and returns them with the app id
// they share. All keys must belong to the same app: the server owns one
// channel namespace, so keys spanning multiple appIds are a misconfiguration
// (DESIGN.md §3).
func parseAPIKeys(specs []keySpec) ([]auth.APIKey, string, error) {
	keys := make([]auth.APIKey, 0, len(specs))
	for _, spec := range specs {
		k, err := auth.ParseAPIKeyWithCapability(spec.key, spec.capability)
		if err != nil {
			return nil, "", err
		}
		keys = append(keys, k)
	}
	appID := keys[0].AppID
	for _, k := range keys[1:] {
		if k.AppID != appID {
			return nil, "", fmt.Errorf("all api keys must share the same appId; %s is not %s", k.AppID, appID)
		}
	}
	return keys, appID, nil
}

// fixtureSpec validates the config file's [[namespaces]] and [[channels]]
// sections and builds the presence-fixture spec to seed at startup
// (DESIGN.md §9, §12.5). A namespace with no id or an unknown mode, a
// channel with no name, or a presence member with no clientId is a
// malformed section and returns an error. Returns a nil spec when no
// channels are declared.
func fixtureSpec(file config.File) (*fixtures.Spec, error) {
	if err := config.ValidateNamespaces(file.Namespaces); err != nil {
		return nil, err
	}
	if len(file.Channels) == 0 {
		return nil, nil
	}
	spec := &fixtures.Spec{Channels: make([]fixtures.Channel, 0, len(file.Channels))}
	for i, ch := range file.Channels {
		if ch.Name == "" {
			return nil, fmt.Errorf("channel #%d has no name", i)
		}
		members := make([]fixtures.Member, 0, len(ch.Presence))
		for mi, m := range ch.Presence {
			if m.ClientID == "" {
				return nil, fmt.Errorf("channel %q presence member #%d has no clientId", ch.Name, mi)
			}
			members = append(members, fixtures.Member{
				ClientID: m.ClientID,
				Data:     m.Data,
				Encoding: m.Encoding,
			})
		}
		spec.Channels = append(spec.Channels, fixtures.Channel{Name: ch.Name, Presence: members})
	}
	return spec, nil
}

// writeAddrFile atomically writes addr to path. It writes to a temp file
// in the same directory and renames it into place, so a parent process
// polling the path reads a complete address rather than a truncated one.
func writeAddrFile(path, addr string) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".addr-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if _, err := tmp.WriteString(addr); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	return os.Rename(name, path)
}

// openStorage constructs the storage.Storage selected by mode:
//
//   - memory: in-process, no persistence.
//   - disk:   bbolt at <dataDir>/ably.db (dataDir created if absent).
//   - cluster: postgres at postgresDSN (auto-migrates schema on Open;
//     spawns the LISTEN/NOTIFY broker — see DESIGN.md §7.2).
//
// ctx bounds the cluster-mode dial + ping + migrate; it's ignored by
// the in-process modes.
// newMux builds the HTTP routing table: the shared module's whole surface at
// the paths the module names, and this server's own endpoints around it.
//
// The module anchors its WebSocket route to the exact root with `{$}`, which
// is what keeps it from being a catch-all: a bare `GET /` matches every
// unmatched GET path in Go's ServeMux and would feed each one to the upgrader,
// returning a confusing 400 with WebSocket headers. With `{$}`, only `/`
// upgrades and unknown paths fall through to a clean 404.
func newMux(shared *handles.Protocol, rs *rest.Server, m *metrics.Metrics, enableStatsStub bool) *http.ServeMux {
	mux := http.NewServeMux()

	// REST routes are wrapped so each records ably_http_requests_total by
	// route pattern / method / status (DESIGN.md §10). The connection routes
	// are excluded — their handlers block for the connection's whole lifetime,
	// which the connection metrics already cover. /metrics itself is
	// unwrapped so scrapes don't inflate the counters.
	rest := func(pattern string, h http.HandlerFunc) {
		mux.Handle(pattern, instrumentHTTP(m, pattern, h))
	}
	// The whole protocol surface is the shared module's, its paths included:
	// the module declares where each of its handlers belongs, so this server
	// and realtime answer the same request at the same URL rather than each
	// choosing. That covers the three transports a client can reach a
	// connection through — WebSocket, SSE and comet — as well as REST, and a
	// route per path for CORS preflights and for a method the path does not
	// take, which would otherwise reach the catch-all below and be reported as
	// a path that does not exist.
	for _, route := range shared.Routes() {
		if strings.HasPrefix(route.Name, "connection.") {
			mux.Handle(route.Pattern, route.Handler)
			continue
		}
		rest(route.Pattern, route.Handler.ServeHTTP)
	}
	// Ably SDKs' REST history reads request /history; serve it as
	// an alias so those reads work.
	rest("GET /channels/{channelId}/history", shared.REST().HandleHistory)

	// What the shared module does not describe stays here: minting a token is
	// this server's own endpoint, and the rest are operational.
	rest("POST /keys/{keyName}/requestToken", rs.HandleRequestToken)
	// GET/POST /stats are a compatibility stub (DESIGN.md §1) only needed by
	// SDK test flows — the sandbox provisioner boots its children with
	// --enable-stats-stub (§17). Unregistered by default, they fall
	// through to the catch-all Ably-shaped 40400 below rather than the
	// stub always answering an app that never asked for it.
	if enableStatsStub {
		rest("GET /stats", rs.HandleStats)
		// POST /stats is a no-op stats-injection stub: SDK test flows write
		// stats before reading them, and a 404 would leave the SDK blocked
		// reading the error body of a request it never fully sent.
		rest("POST /stats", rs.HandlePostStats)
	}
	rest("GET /healthz", rs.HandleHealthz)
	rest("GET /readyz", rs.HandleReadyz)

	// Catch-all fallback: any path/method not matched above gets an
	// Ably-shaped 404 (code 40400) instead of ServeMux's bare 404, and —
	// since a subtree "/" pattern also matches paths whose only registered
	// method differs — the 405 Go would otherwise return (e.g. GET on the
	// POST-only requestToken) becomes the 404 SDKs expect (DESIGN.md §2.2).
	// "GET /{$}" stays more specific, so the WebSocket root is unaffected.
	rest("/", rs.HandleNotFound)

	// /metrics is deliberately not registered here: it moved to the debug
	// listener (newDebugMux, --debug-listen) so a publicly reachable main
	// listener doesn't leak operational detail to anyone
	// (DESIGN.md §10). An unmatched GET /metrics on the main mux falls
	// through to the catch-all above and gets the Ably-shaped 404.
	return mux
}

// newDebugMux builds the routing table served on the --debug-listen
// address: net/http/pprof's handlers (registered on http.DefaultServeMux
// by this file's blank import) via a catch-all delegate, plus /metrics
// registered directly and unwrapped — no instrumentHTTP — so scraping it
// doesn't inflate ably_http_requests_total (DESIGN.md §10). m is nil only
// in tests that pass no Metrics; production always wires one.
func newDebugMux(m *metrics.Metrics) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/", http.DefaultServeMux)
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

func openStorage(ctx context.Context, mode, dataDir, postgresDSN string) (storage.Storage, error) {
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
		if postgresDSN == "" {
			return nil, fmt.Errorf("--postgres-dsn is required when --mode=cluster (env: %s)", postgresDSNEnv)
		}
		return postgres.Open(ctx, postgres.Options{DSN: postgresDSN})
	default:
		return nil, fmt.Errorf("unknown --mode %q (valid: memory, disk, cluster)", mode)
	}
}

// newLogger builds the process logger. level is parsed by
// logging.ParseLevel (trace..error); format selects the slog handler:
// "text" (the default) or "json". Either being unrecognised is a
// startup error.
func newLogger(level, format string, w io.Writer) (*logging.Logger, error) {
	lvl, err := logging.ParseLevel(level)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{Level: lvl, ReplaceAttr: logging.ReplaceAttr}
	switch format {
	case "text":
		return logging.New(slog.NewTextHandler(w, opts)), nil
	case "json":
		return logging.New(slog.NewJSONHandler(w, opts)), nil
	default:
		return nil, fmt.Errorf("unknown --log-format %q (valid: text, json)", format)
	}
}

// hasVersionFlag reports whether args ask for --version. Scanned by
// hand, ahead of flag.Parse, because Run answers it before it has a
// FlagSet — or a loadable config to build one from.
func hasVersionFlag(args []string) bool {
	for _, a := range args {
		switch {
		case a == "--":
			return false
		case a == "--version", a == "-version":
			return true
		case strings.HasPrefix(a, "--version="), strings.HasPrefix(a, "-version="):
			// A bool flag also accepts --version=false, which asks
			// to run the server. A value flag.Parse would reject is
			// left to it to report.
			v, err := strconv.ParseBool(a[strings.Index(a, "=")+1:])
			return err == nil && v
		}
	}
	return false
}
