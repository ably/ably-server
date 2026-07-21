// Command ably-server is the open-source Ably-compatible server.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/ably/ably-server/internal/server"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(server.Run(ctx, server.Opts{
		Args:   os.Args[1:],
		Getenv: os.Getenv,
		Out:    os.Stdout,
	}))
}
