# M2 — Node embedded-server (primary track): measured trade-offs

Measured on macOS arm64 (Darwin 25.5.0), Node v24.11.0, Go 1.25.11,
ably-js 2.x, loopback. The Node host app embeds the prebuilt `ably-server`
binary as a supervised child process and reverse-proxies all realtime
(WebSocket) + REST traffic to it on a dedicated port (EMBEDDING-POC.md §5,
§6). Express **and** Fastify adapters are provided.

## Headline result

The Go conformance harness passes through **both** Express and Fastify —
all six scenarios — and the **unmodified `ably-js` SDK** does a pub/sub
round-trip through each proxy.

| | Express | Fastify |
|---|---|---|
| harness | **PASS 6/6** | **PASS 6/6** |
| connect | 4.17 ms | 4.20 ms |
| pubsub mean / p99 | 0.15 / 0.18 ms | 0.19 / 0.28 ms |
| restpubsub mean / p99 | 0.67 / 0.94 ms | 1.97 / 2.41 ms |
| history | 10, ordered | 10, ordered |
| resume-after-drop | RESUMED, lossless | RESUMED, lossless |
| soak (200 msgs) | 0 lost, ~1005 msg/s | 0 lost, ~542 msg/s |
| native ably-js smoke | **PASS** | **PASS** |

Raw reports: `../results/node-express.json`, `../results/node-fastify.json`.
Fastify's REST path is slower because `@fastify/http-proxy` (reply-from)
re-frames each request; Express's `http-proxy-middleware` streams it.

## Server changes this track required (the key finding)

Proving the **unmodified ably-js** SDK forced two server-side fixes that
`ably-go` and the .NET SDK did not need (they tolerate the gaps). These are
real protocol-correctness fixes, not embedding concerns — committed
separately and recommended for promotion to proper server tasks:

1. **`connectionDetails` (Ably CD2) on CONNECTED.** ably-js treats a
   CONNECTED frame without `connectionDetails` as a protocol error and
   refuses the connection. Added `protocol.ConnectionDetails` and populate
   it (clientId, connectionKey, maxIdleInterval) on connect.
2. **`msgSerial` must be a pointer.** `MsgSerial int64` with
   `json:"...,omitempty"` silently drops `msgSerial=0` — the FIRST publish
   on every connection — from the ACK. ably-js then cannot correlate the
   ack to its pending publish and the publish promise hangs forever.
   Changed to `*int64` so a meaningful zero serialises. ably-go happened
   not to depend on the echoed 0; ably-js does.

Both are covered by the existing server test suite (migrated to the pointer
API, assertions unchanged) — `go test ./...` is green.

## §7 trade-off measurements

### Developer efficiency (time-to-first-message)
- Steps from `npm install` to a working pub/sub round-trip following the
  README: **4** — (1) `npm install`, (2) build/obtain the binary, (3) ~8
  lines of glue (server + proxy), (4) point ably-js at the port.
- The binary build (`go build`) is a one-time input; in a real package the
  binary ships via npm `optionalDependencies` per os/cpu (esbuild precedent)
  so the host dev never runs Go.

### Developer experience (DX)
- **Integration glue ≈ 8 lines** for the host dev: `new AblyServer()` +
  `await server.start()`, `mountAblyProxy(app, { server })` (or
  `registerAblyProxy` for Fastify), `app.listen`, and
  `httpServer.on('upgrade', proxy.upgrade)` for Express.
- New concepts: ~2 (a supervised child, and wiring the WS `upgrade` event —
  the one non-obvious Express step; Fastify hides it inside the plugin).
- Fits idiomatic patterns: yes — Express middleware and a Fastify plugin.
- **Misconfiguration error quality (captured):** a missing binary fails fast
  with `ably-server binary not found at <path>. Build it first (go build -o
  bin/ably-server ./cmd/ably-server) or set ABLY_SERVER_BINARY to its path.`

### Platform portability
- Prebuilt binary, ~15.5 MB per platform; install+run needs **no toolchain**
  on the user's machine (the binary is self-contained). Multi-platform =
  one `optionalDependencies` package per os/cpu.
- macOS Gatekeeper notarisation / Windows code-signing are real
  productisation costs (out of PoC scope; on the cost ledger).

### Ease of use
- One-liner to start: `new AblyServer()` + `await server.start()` (or the
  `await startEmbeddedServer()` convenience), then mount the proxy.
- Auto free-port: yes (the embedded server picks one; the proxy reads it live,
  so it still works after a restart onto a new port).
- Auto-shutdown with app: yes (wired in the example's SIGTERM/SIGINT handler).
- Config surface: `{ apiKey, mode, binaryPath, maxRestarts, ... }` + the
  `ABLY_SERVER_BINARY` / `ABLY_SERVER_API_KEY` env vars.

### Reliability (measured)
- Harness incl. resume + 200-msg zero-loss soak: PASS through both proxies.
- **Restart-on-crash:** `kill -9` the embedded child → the embedded server logs
  `restart attempt 1` → re-ready on the same port → a fresh harness
  (connect, pubsub) PASSES. Verified with exact-PID tracking (child
  86591 → SIGKILL → respawn 86615 → harness PASS).
- **Graceful shutdown:** SIGTERM the host app → exits cleanly (code 0) in
  ~0.1 s with **no orphaned child**, after real traffic.
  - *Bug found and fixed here:* the first cut closed the HTTP server before
    stopping the child and hung indefinitely after traffic (a proxied socket
    lingered; a live ably-js client would also reconnect into the closing
    server). Fix: **stop the child first** (tears down the proxied upstreams
    so the proxy closes the client sockets), *then* close the server. See
    `examples/express-app.js`.

### Complexity / operational cost
- Moving parts: **two processes** (host app + child) vs Go's one.
- Supervision burden: the package owns it (spawn, /readyz, restart, stop).
- Packaging: per-os/cpu binary packages (esbuild model) — more complex than
  Go's "just a dependency", simpler than a native addon.
- Failure blast radius: **isolated** — a server crash does not take the host
  app down (the embedded server restarts the child). This is the main advantage
  over the Go in-process ceiling.

## How to reproduce

```sh
cd experiments/embedding/node
npm install
go build -o bin/ably-server ../../../cmd/ably-server
go build -o ../harness/harness ../../../experiments/embedding/harness
ABLY_PUBLIC_PORT=8541 node examples/express-app.js &      # or examples/fastify-app.js
../harness/harness --port 8541 --label node-express --json
ABLY_PUBLIC_PORT=8541 node examples/ably-js-smoke.js       # native ably-js
```
