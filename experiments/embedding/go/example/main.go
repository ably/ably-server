// Command example is a tiny Go host app that embeds ably-server in-process
// (EMBEDDING-POC.md §8 M1) and serves it on a dedicated port. This file is
// the entire integration glue a Go developer writes: import the package,
// New, mount the handler, defer Close. An unmodified Ably SDK then points
// at this host+port.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ably/ably-server/experiments/embedding/go/ablyembed"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8500", "dedicated port for the embedded Ably endpoint")
	key := flag.String("api-key", env("ABLY_SERVER_API_KEY", "app.key:secret"), "API key appId.keyId:keySecret")
	flag.Parse()

	// --- the glue: embed ably-server in-process ---
	embedded, err := ablyembed.New(ablyembed.Options{APIKey: *key})
	if err != nil {
		log.Fatalf("embed ably-server: %v", err)
	}
	defer embedded.Close()

	srv := &http.Server{Addr: *listen, Handler: embedded.Handler, ReadHeaderTimeout: 10 * time.Second}
	// --- end glue ---

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("embedded ably-server listening on %s", *listen)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
