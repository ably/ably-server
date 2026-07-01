# M1 — Go in-process embed (the ceiling)

Measured on macOS arm64 (Darwin 25.5.0), Go 1.25.11, ably-go v1.4.0,
loopback. This track is the **best-case** reference (lowest glue, lowest
overhead) the managed-runtime tracks are measured against.

## What it is

A Go host app imports `ablyembed`, calls `New`, and serves the returned
`http.Handler` on its own listener. No child process, no reverse proxy, no
FFI. Realtime still flows over a real local socket because that is how the
host serves the handler. The embedded server is literally ably-server's
own `internal/core`, `realtime`, `rest`, and `storage/memory` wired the
same way `cmd/ably-server/main.go` wires them — no change to ably-server.

## §7 trade-off measurements

### Developer efficiency (time-to-first-message)
- Steps from nothing to a working pub/sub round-trip: **3** — (1) import
  `ablyembed`, (2) `New` + mount `Handler` on an `http.Server`, (3) point
  an SDK at host+port. No install step (it is a Go import).
- Wall-clock dominated by `go build` of the host app (~1s incremental).

### Developer experience (DX)
- **Integration glue: 3 essential lines** (`New`, mount `Handler`, `defer
  Close`); 6 lines including an explicit `http.Server` + graceful
  shutdown. See `example/main.go` between the glue markers.
- New concepts a dev must hold: **~1** (call `New`, mount a handler — both
  idiomatic net/http).
- Fits idiomatic Go: yes — it is just an `http.Handler`. Composes with any
  router/middleware; can be mounted alongside the app's own routes.
- Misconfiguration error quality: an invalid key fails fast at `New`
  (`ablyembed.New: <auth.ParseAPIKey error>`), before serving — surfaced
  as a normal Go error, not a late runtime failure.

### Platform portability
- Covers every platform the Go toolchain targets (the host app compiles
  ably-server in). No prebuilt-binary matrix, no extraction, no toolchain
  beyond the Go compiler the host already uses.
- **Binary size: host app ≈ 10.2 MB** (decimal MB) with ably-server compiled
  in (memory + disk storage). Smaller than the **≈16.5 MB** standalone binary
  because the in-process embed imports only `storage/memory` and
  `storage/bbolt`, not the Postgres (pgx) cluster backend. Embedding adds
  ~10 MB to a Go app.

### Ease of use
- One-liner to start: yes (`srv.ListenAndServe()` after `New`).
- Auto-port: the host owns the listener, so it picks the port (or `:0`).
- Auto-shutdown with the app: inherent — `defer Close()` + `srv.Shutdown`.
- Config surface: a single `Options{APIKey, HeartbeatInterval, Logger}`.

### Reliability (measured)
Harness through the in-process mount — **all six scenarios PASS** (JSON):

| scenario | result |
|---|---|
| connect | 1.63 ms |
| pubsub (round-trip) | min 0.08 / mean 0.11 / p50 0.10 / p99 0.16 / max 0.16 ms (20 samples) |
| restpubsub (REST→WS) | min 0.19 / mean 0.29 / p50 0.23 / p99 0.65 ms (10 samples) |
| history | 10 messages, ordered |
| resume-after-drop | RESUMED flag set, 3/3 gap delivered, lossless |
| soak | 200 messages, 0 lost, 0 duplicates, ~2247 msg/s |

- msgpack (binary) protocol also passes in-process.
- **Graceful shutdown**: SIGTERM → host app exits cleanly (code 0).
- **Server crash / restart**: N/A by construction — there is no child to
  restart. This is the ceiling's defining trade-off: zero supervision
  burden, but a panic in the server shares the host app's blast radius
  (EMBEDDING-POC.md §5). A real deployment would rely on the host app's
  own process supervisor (systemd/k8s) to restart the whole app.

Raw report: `../results/go-inproc.json`.

### Complexity / operational cost
- Moving parts added: **zero** — one process, one binary.
- Supervision burden: none (no child).
- Packaging complexity: none — it ships as a normal Go dependency.
- Failure blast radius: **shared** (in-process). The one real cost of the
  ceiling vs a supervised child.

## How to reproduce

```sh
go build -o /tmp/m1-example ./experiments/embedding/go/example
ABLY_SERVER_API_KEY=app.key:secret /tmp/m1-example --listen 127.0.0.1:8500 &
go build -o experiments/embedding/harness/harness ./experiments/embedding/harness
experiments/embedding/harness/harness --port 8500 --label go-inproc --json
```
