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
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/realtime"
)

const apiKeyEnv = "ABLY_SERVER_API_KEY"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Getenv, os.Stderr))
}

// run executes the server and returns the process exit code. All
// inputs (args, environment, output) are passed in so the function is
// testable without touching package-level state.
func run(ctx context.Context, args []string, getenv func(string) string, stderr io.Writer) int {
	fs := flag.NewFlagSet("ably-server", flag.ContinueOnError)
	fs.SetOutput(stderr)
	listen := fs.String("listen", ":8080", "address for HTTP/WS listener")
	apiKey := fs.String("api-key", getenv(apiKeyEnv), "API key in appId.keyId:keySecret format (env: "+apiKeyEnv+")")
	hbInterval := fs.Duration("heartbeat-interval", realtime.DefaultHeartbeatInterval, "server-driven HEARTBEAT cadence")
	shutdownGrace := fs.Duration("shutdown-grace", 10*time.Second, "window to disconnect existing connections on SIGTERM")
	logLevel := fs.String("log-level", "info", "log level: debug, info, warn, error")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	logger := newLogger(*logLevel, stderr)

	if *apiKey == "" {
		fmt.Fprintf(stderr, "--api-key (or %s) is required\n", apiKeyEnv)
		return 1
	}
	parsedKey, err := auth.ParseAPIKey(*apiKey)
	if err != nil {
		fmt.Fprintf(stderr, "invalid api key: %v\n", err)
		return 1
	}

	rt := realtime.NewServer(realtime.Config{
		Key:               parsedKey,
		HeartbeatInterval: *hbInterval,
		Logger:            logger,
	})

	mux := http.NewServeMux()
	mux.Handle("/", rt)

	srv := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", *listen)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("listener exited", "err", err)
			return 1
		}
		return 0
	case <-ctx.Done():
		logger.Info("shutdown signal received")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownGrace)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown error", "err", err)
			return 1
		}
		return 0
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
