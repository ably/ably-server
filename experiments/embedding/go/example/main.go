// Command example is a tiny Go host app that embeds ably-server in-process
// (EMBEDDING-POC.md §8 M1) and serves it on a dedicated port. The Ably
// integration is the few lines between the glue markers: import the
// package, New, mount the handler, defer Close. An unmodified Ably SDK
// then points at this host+port.
//
// It optionally also serves the browser demo at /demo/ (pass --demo-dir),
// which is just one of the host app's own routes sitting next to Ably.
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
	mode := flag.String("mode", "memory", "storage backend: memory (ephemeral) or disk (durable, needs --data-dir)")
	dataDir := flag.String("data-dir", "", "data directory for disk mode (holds the bbolt file)")
	demoDir := flag.String("demo-dir", "", "if set, serve the browser demo from this dir at /demo/")
	flag.Parse()

	// --- the glue: embed ably-server in-process ---
	embedded, err := ablyembed.New(ablyembed.Options{APIKey: *key, Mode: *mode, DataDir: *dataDir})
	if err != nil {
		log.Fatalf("embed ably-server: %v", err)
	}
	defer embedded.Close()
	// --- end glue ---

	// Ably owns the root (WebSocket at /, REST under /channels …). The host
	// app's own routes — here, the optional demo page — sit alongside it.
	var handler http.Handler = embedded.Handler
	if *demoDir != "" {
		mux := http.NewServeMux()
		mux.Handle("/demo/", http.StripPrefix("/demo/", http.FileServer(http.Dir(*demoDir))))
		mux.Handle("/", embedded.Handler)
		handler = mux
		log.Printf("serving browser demo at http://%s/demo/", *listen)
	}

	srv := &http.Server{Addr: *listen, Handler: handler, ReadHeaderTimeout: 10 * time.Second}

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
