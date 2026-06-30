# Embedding ably-server in a Go app (in-process)

The Go track of the embedding PoC. In Go there is no binding and no child
process: import the package, mount the handler.

## Use it

```go
import "github.com/ably/ably-server/experiments/embedding/go/ablyembed"

embedded, err := ablyembed.New(ablyembed.Options{APIKey: "app.key:secret"})
if err != nil {
    log.Fatal(err)
}
defer embedded.Close()

// Serve Ably on a dedicated port; point an unmodified SDK at host+port.
http.ListenAndServe("127.0.0.1:8500", embedded.Handler)
```

`embedded.Handler` is a plain `http.Handler` exposing the full ably-server
surface (WebSocket at `/`, REST under `/channels`, `/keys`, `/time`,
`/healthz`, `/readyz`). Mount it on its own port, or compose it into a
larger router.

Point any unmodified Ably SDK at it by host + port (EMBEDDING-POC.md §6):

```go
ably.NewRealtime(
    ably.WithKey("app.key:secret"),
    ably.WithEndpoint("127.0.0.1"), ably.WithPort(8500), ably.WithTLS(false),
    ably.WithInsecureAllowBasicAuthWithoutTLS(), ably.WithUseTokenAuth(false),
)
```

## Run the example + conformance harness

```sh
go build -o /tmp/m1-example ./experiments/embedding/go/example
ABLY_SERVER_API_KEY=app.key:secret /tmp/m1-example --listen 127.0.0.1:8500 &
go build -o experiments/embedding/harness/harness ./experiments/embedding/harness
experiments/embedding/harness/harness --port 8500 --label go-inproc --json
```

See [NOTES.md](NOTES.md) for the measured trade-offs.

## Layout

- `ablyembed/` — the embed package (`New` → `Embedded{Handler, Close}`).
- `example/` — a ~6-line host app that mounts it and serves it.
