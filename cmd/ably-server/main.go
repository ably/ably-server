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
	listen := flag.String("listen", ":8080", "address for HTTP/WS listener")
	apiKey := flag.String("api-key", os.Getenv(apiKeyEnv), "API key in appId.keyId:keySecret format (env: "+apiKeyEnv+")")
	hbInterval := flag.Duration("heartbeat-interval", realtime.DefaultHeartbeatInterval, "server-driven HEARTBEAT cadence")
	shutdownGrace := flag.Duration("shutdown-grace", 10*time.Second, "window to disconnect existing connections on SIGTERM")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	flag.Parse()

	logger := newLogger(*logLevel)
	slog.SetDefault(logger)

	if *apiKey == "" {
		fatal(logger, fmt.Sprintf("--api-key (or %s) is required", apiKeyEnv))
	}
	parsedKey, err := auth.ParseAPIKey(*apiKey)
	if err != nil {
		fatal(logger, fmt.Sprintf("invalid api key: %v", err))
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", *listen)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("listener exited", "err", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		logger.Info("shutdown signal received")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownGrace)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown error", "err", err)
			os.Exit(1)
		}
	}
}

func fatal(logger *slog.Logger, msg string) {
	logger.Error(msg)
	os.Exit(1)
}

func newLogger(level string) *slog.Logger {
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
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}
