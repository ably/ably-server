// Command example-subpath embeds ably-server in-process and mounts it
// under a SUBPATH (/ably) alongside the host app's own routes — the
// "future" shape from EMBEDDING-POC.md §6, shown here because in Go the
// server side is trivial: wrap the handler in http.StripPrefix.
//
// Caveat (honest): stock Ably SDKs build root-rooted URLs and have no
// basePath option yet, so they cannot target /ably today. This example
// proves the SERVER + host routing work under a subpath; the conformance
// harness verifies it with `--base-path /ably` (raw protocol). Once the
// SDKs gain a basePath option, a browser/SDK client would point at /ably.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ably/ably-server/experiments/embedding/go/ablyembed"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8501", "host app address")
	mountAt := flag.String("mount", "/ably", "subpath to mount the embedded Ably endpoint under")
	key := flag.String("api-key", env("ABLY_SERVER_API_KEY", "app.key:secret"), "API key appId.keyId:keySecret")
	flag.Parse()

	embedded, err := ablyembed.New(ablyembed.Options{APIKey: *key})
	if err != nil {
		log.Fatalf("embed ably-server: %v", err)
	}
	defer embedded.Close()

	mux := http.NewServeMux()
	// The host app's own routes live at the root...
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, "host app home — Ably is embedded under %s/\n", *mountAt)
	})
	// ...and Ably is mounted under the subpath. StripPrefix re-roots the
	// request so the embedded mux (which expects /, /channels, ...) matches.
	mux.Handle(*mountAt+"/", http.StripPrefix(*mountAt, embedded.Handler))

	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		log.Printf("host app on %s; embedded Ably under %s/", *listen, *mountAt)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
