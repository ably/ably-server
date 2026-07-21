// Command ably-local-sandbox is a disposable-instance provisioner for the Ably
// SDK test suites (DESIGN.md §15). It implements the sandbox admin API
// (POST /apps, DELETE /apps/{appId}, POST /stats) by booting one
// ably-server child per provisioned app, keeping the core server strictly
// single-app. Its working name is provisional, pending a final naming decision.
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
	"syscall"
	"time"

	"github.com/ably/ably-server/internal/logging"
)

const (
	listenEnv    = "ABLY_LOCAL_SANDBOX_LISTEN"
	serverBinEnv = "ABLY_LOCAL_SANDBOX_SERVER_BIN"
	idleTTLEnv   = "ABLY_LOCAL_SANDBOX_IDLE_TTL"
	logDirEnv    = "ABLY_LOCAL_SANDBOX_LOG_DIR"
	logLevelEnv  = "ABLY_LOCAL_SANDBOX_LOG_LEVEL"
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

// runOpts bundles run's inputs so production main() and tests share one
// entry point, mirroring cmd/ably-server. Ready is an optional testing
// hook that receives the bound listener address once serving starts.
type runOpts struct {
	Args   []string
	Getenv func(string) string
	Out    io.Writer
	Ready  chan<- net.Addr
}

// run executes the provisioner and returns the process exit code. On
// SIGTERM it tears down every child before returning.
func run(ctx context.Context, opts runOpts) int {
	fs := flag.NewFlagSet("ably-local-sandbox", flag.ContinueOnError)
	fs.SetOutput(opts.Out)
	listen := fs.String("listen", def(opts.Getenv(listenEnv), ":9080"), "address for the provisioning HTTP listener (env: "+listenEnv+")")
	serverBin := fs.String("server-bin", opts.Getenv(serverBinEnv), "path to the ably-server binary to boot per app; defaults to a sibling of this executable, else 'go run ./cmd/ably-server' (env: "+serverBinEnv+")")
	idleTTL := fs.Duration("idle-ttl", defDuration(opts.Getenv(idleTTLEnv), 30*time.Minute), "kill child servers not provisioned or deleted within this window (env: "+idleTTLEnv+")")
	logDir := fs.String("log-dir", opts.Getenv(logDirEnv), "directory for per-app child configs and logs; a temp dir is created if empty (env: "+logDirEnv+")")
	logLevel := fs.String("log-level", def(opts.Getenv(logLevelEnv), "info"), "log level: "+logging.LevelNames+" (env: "+logLevelEnv+")")
	if err := fs.Parse(opts.Args); err != nil {
		return 2
	}

	logger, err := newLogger(*logLevel, opts.Out)
	if err != nil {
		fmt.Fprintln(opts.Out, err)
		return 1
	}

	serverCmd, err := resolveServerCommand(*serverBin)
	if err != nil {
		logger.Error("resolve ably-server binary", "err", err)
		return 1
	}
	logger.Info("child server command", "name", serverCmd.name, "prefix", serverCmd.prefixArgs)

	dir := *logDir
	if dir == "" {
		dir, err = os.MkdirTemp("", "ably-local-sandbox-*")
		if err != nil {
			logger.Error("create log dir", "err", err)
			return 1
		}
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		logger.Error("create log dir", "dir", dir, "err", err)
		return 1
	}

	p := newProvisioner(serverCmd, dir, *idleTTL, logger)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /apps", p.handleCreateApp)
	mux.HandleFunc("DELETE /apps/{appId}", p.handleDeleteApp)
	mux.HandleFunc("POST /stats", p.handlePostStats)

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}

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

	go p.reapLoop(ctx)

	go func() {
		logger.Info("listening", "addr", listener.Addr().String(), "logDir", dir)
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serve error", "err", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received; terminating children")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown error", "err", err)
	}
	p.shutdown()
	return 0
}

// def returns v if non-empty, else fallback.
func def(v, fallback string) string {
	if v != "" {
		return v
	}
	return fallback
}

// defDuration parses v as a duration, falling back to the given default
// when v is empty or malformed.
func defDuration(v string, fallback time.Duration) time.Duration {
	if v == "" {
		return fallback
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	return fallback
}

// newLogger builds the process logger with a text handler at the given
// level. Any unrecognised level is a startup error.
func newLogger(level string, w io.Writer) (*logging.Logger, error) {
	lvl, err := logging.ParseLevel(level)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{Level: lvl, ReplaceAttr: logging.ReplaceAttr}
	return logging.New(slog.NewTextHandler(w, opts)), nil
}
