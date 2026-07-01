# Embedding ably-server: PoC results

> Output of the embedding PoC (EMBEDDING-POC.md). Every number here was
> **measured** on this machine, not estimated. Raw harness reports are in
> [`results/`](results/); per-track detail is in each track's `NOTES.md`.

## Bottom line

**Embedding `ably-server` as a dependency of a non-Go web app works, and is
low-friction across all four ecosystems.** An unmodified Ably SDK
connects *through the host app* to an embedded server and does pub/sub,
history, and resume-after-drop over a real local socket. The conformance
harness passes 6/6 in **Go (in-process), Node (Express and Fastify), .NET
(YARP), and Python (FastAPI/Starlette)**; the embedded server survives
`kill -9` and is auto-restarted; graceful shutdown is clean with no orphaned
process.

Two caveats worth productising around, both surfaced by *running* it:

1. **SDK strictness varies.** `ably-go` works against the server as-is.
   `ably-js` does **not** until the server emits `connectionDetails` (CD2)
   and serialises `msgSerial=0` — two small, real server fixes this PoC
   implemented and verified (TASK-68, TASK-69). The .NET `IO.Ably` SDK does
   realtime fine but refuses **key-auth REST over plain HTTP** (works with
   TLS or token auth).
2. **Same-origin mounting still needs an SDK `basePath` option** (deferred,
   §6). The PoC uses a dedicated port, which works with stock SDKs today.

**Recommendation: worth productising**, Node and Go first, .NET close
behind. See [§ Recommendation](#recommendation).

## What was tested

| Track | Model | SDK exercised | Dir |
|---|---|---|---|
| **Floor** (baseline) | SDK → standalone `ably-server --mode memory` | ably-go | — |
| **Ceiling** (M1) | Go host imports the package, mounts the handler **in-process** | ably-go | [`go/`](go/) |
| **Node** (M2) | Host supervises the binary as a child, **reverse-proxies** (Express + Fastify) | ably-go + **ably-js** | [`node/`](node/) |
| **.NET** (M3) | Host supervises the binary as a child, **YARP** reverse-proxy | ably-go + **IO.Ably** | [`dotnet/`](dotnet/) |
| **Python** (TASK-70) | Host supervises the binary as a child, **ASGI** reverse-proxy (FastAPI/Starlette) | ably-go + **ably-python** | [`python/`](python/) |

The **conformance harness** ([`harness/`](harness/)) is one Go binary run
against every track by host+port, so the numbers are apples-to-apples. Its
six scenarios — connect, pubsub (realtime round-trip), restpubsub (REST→WS),
history, resume-after-drop, soak — use the unmodified `ably-go` SDK, except
`resume`, which uses the raw wire protocol so the drop is deterministic and
proxy-independent. Each managed-runtime track additionally runs a **native
SDK smoke** (ably-js / IO.Ably / ably-python) to prove that ecosystem's own
SDK works through the proxy.

Environment: macOS 15 (Darwin 25.5.0) arm64, Go 1.25.11, Node v24.11.0,
.NET SDK 10.0.301, Python 3.14.5, ably-go v1.4.0, ably-js 2.x, IO.Ably
1.2.18, ably-python 3.1.2. `memory` mode by default, with `disk` mode also
exercised for durability (see Reliability); zero external dependencies. All
loopback.

## The trade-off matrix (§7)

| Dimension | Floor (standalone) | Go in-process (ceiling) | Node (Express / Fastify) | .NET (YARP) | Python (FastAPI) |
|---|---|---|---|---|---|
| **Dev efficiency** — steps to first message | run 1 binary | **3** (import → mount handler → point SDK) | **4** (`npm i` → binary → ~8 lines → point SDK) | **3** from prebuilt (`dotnet build` → run → point SDK); ~1.6 s warm | **4** (venv+pip → binary → ~20 lines → point SDK) |
| **DX** — integration glue | n/a | **~3 lines** (`New` + mount `Handler` + `Close`) | **~8 lines** (`AblyServer` + proxy + WS `upgrade` wiring) | **~20 lines** (host builder + YARP route + `AddAblyEmbedded`) | **~20 lines** (lifespan `AblyServer` + `make_proxy_app` + ASGI fall-through) |
| **DX** — new concepts | n/a | ~1 (it's an `http.Handler`) | ~2 (child process; wire WS `upgrade`) | ~2 (child process; YARP cluster destination) | ~2 (child process; ASGI fall-through routing) |
| **DX** — idiomatic fit | n/a | native `net/http` | Express middleware / Fastify plugin | ASP.NET + YARP config | FastAPI/Starlette ASGI (WSGI can't do WS) |
| **Portability** — binary (decimal MB) | ~16.5 MB standalone | **+10.2 MB compiled into the host binary** (memory + disk), any Go target, no extra toolchain | ~16.5 MB prebuilt **per os/cpu**, no toolchain on user machine | ~16.5 MB prebuilt + **.NET runtime**; NuGet `runtimes/<rid>` | ~16.5 MB prebuilt + **Python runtime**; PyPI platform wheels |
| **Ease of use** — auto free-port | — | host owns the port | **yes** (`AblyServer`; proxy reads it live) | **yes** (`AblyServer`; injected into YARP) | **yes** (`AblyServer`; proxy reads it live) |
| **Ease of use** — auto-shutdown with app | — | inherent (`defer Close`) | yes (SIGTERM handler) | yes (host lifetime) | yes (ASGI lifespan) |
| **Reliability** — harness 6/6 | **PASS** | **PASS** | **PASS / PASS** | **PASS** | **PASS** |
| **Reliability** — server crash → auto-restart | n/a | **N/A** (no child) | **yes** (kill -9 → respawn → harness passes) | **yes** (kill -9 → respawn → harness passes) | **yes** (kill -9 → respawn → harness passes) |
| **Reliability** — graceful shutdown | n/a | clean (exit 0) | clean (exit 0, ~0.1 s, no orphan) | clean (exit 0, no orphan) | clean (exit 0, no orphan) |
| **Complexity** — processes | 1 | **1** | 2 | 2 | 2 |
| **Complexity** — failure blast radius | n/a | **shared** (in-process) | **isolated** (child) | **isolated** (child) | **isolated** (child) |
| **Native SDK through host** | — | ably-go | **ably-js ✓** (after the 2 server fixes) | **IO.Ably realtime ✓**; key-auth REST-over-HTTP ✗ | **ably-python realtime ✓** (key auth, no token) |

### Measured conformance (all tracks, same harness)

One committed run, regenerated directly from `results/*.json` (all six
measured in a single batch on macOS arm64). **Latency figures are indicative
single-run numbers, not benchmarks** — throughput in particular varies run to
run (a second run moved soak by 10–30%); treat orders of magnitude, not
exact values.

| Scenario | Floor | Go in-proc | Node Express | Node Fastify | .NET YARP | Python FastAPI |
|---|---|---|---|---|---|---|
| connect (ms) | 2.6 | 1.2 | 1.7 | 3.7 | 5.6 | 4.5 |
| pubsub mean / p99 (ms) | 0.10 / 0.23 | 0.11 / 0.31 | 0.10 / 0.19 | 0.19 / 0.31 | 0.18 / 0.22 | 0.32 / 0.36 |
| restpubsub mean (ms) | 0.21 | 0.23 | 0.71 | 3.12 | 0.49 | 1.47 |
| history (10, ordered) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| resume (RESUMED, lossless) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| soak (200 msgs, loss / dupes) | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| soak throughput (msg/s, single run) | ~1980 | ~1935 | ~720 | ~515 | ~1485 | ~990 |

Reading the numbers: the embedding **proxy overhead is small** — low
single-digit millisecond connects and sub-millisecond pub/sub round-trips even
through a reverse proxy, and zero message loss everywhere. Node Fastify's REST
path is the slowest because `@fastify/http-proxy`
re-frames each request (`reply-from`) where Express's `http-proxy-middleware`
streams it. None of this is a correctness or scale concern for the PoC's
purpose; it is the cost of a local proxy hop, and it is low.

## Key findings

### Transport coverage
`ably-server` exposes **WebSocket** (`GET /`) and **REST** only. There is
**no comet/SSE fallback** (`/comet`, `/sse`, `/stream` return 404). The
cross-transport reliability leg is therefore scoped to WebSocket + REST.
This is fine for the embedding model; a non-WebSocket fallback would only
matter behind WS-hostile intermediaries, which a localhost embed never has.

### Unmodified-SDK compatibility (the most important finding)
"Unmodified SDK" held — but *which* unmodified SDK matters:

- **ably-go**: works against the server as-is, in every track.
- **ably-js**: refused to connect until two server fixes (below). This is
  the headline: the embedding model is sound, but the **server's protocol
  completeness** is what gates a given SDK, independent of embedding.
- **IO.Ably (.NET)**: realtime pub/sub works unmodified through YARP
  (CONNECTED ~54 ms, round-trip ~6 ms). Its REST client **refuses key
  (basic) auth over plain HTTP** (`EnsureSecureConnection`, no opt-out) —
  use TLS, or token auth (which works through YARP). Realtime is unaffected.

### Server fixes this PoC required and implemented
Both are genuine protocol-correctness fixes, committed and covered by the
existing server test suite (`go test ./...` green). Filed for promotion to
mainline as TASK-68 / TASK-69.

1. **`connectionDetails` (CD2) on CONNECTED** — ably-js treats its absence
   as a protocol error and refuses the connection.
   ([`internal/realtime/connection.go`](../../internal/realtime/connection.go))
2. **`msgSerial` as `*int64`** — as `int64`+`omitempty` it dropped
   `msgSerial=0` (the first publish on every connection) from the ACK, so
   ably-js could never correlate the ack and `publish()` hung forever.
   ([`internal/protocol/message.go`](../../internal/protocol/message.go))

### A real bug found by fault-injecting the Node host
The Express example first closed the HTTP server *before* stopping the
child and hung indefinitely on SIGTERM after real traffic (a proxied socket
lingered; a live ably-js client also auto-reconnects into the closing
server). Fix: **stop the child first**, then close the server. Verified:
clean exit 0 in ~0.1 s, no orphan. This is exactly the kind of operational
sharp edge the "run it, don't describe it" quality bar exists to catch.

### Routing decision (§6) confirmed
The dedicated-port model works with stock SDKs (`restHost`/`realtimeHost`/
`port`/`tls:false` for ably-js; `WithEndpoint`/`WithPort`/`WithTLS(false)`
for ably-go; `RealtimeHost`/`RestHost`/`Port`/`Tls=false` for IO.Ably).
Same-origin subpath mounting (`/ably`) remains blocked on an SDK `basePath`
option and is correctly deferred.

## A note on auth

For local dev over plain `http://`, you'd reach for API-key (basic) auth —
but **Ably's spec (RSC18/TO3d) requires TLS for an API key**, because the key
is a long-lived secret that would otherwise travel in cleartext. That secure
default is correct; what differs across SDKs is whether they give you a
*conscious dev opt-out*:

| SDK | Key (basic) auth over plain HTTP | Dev opt-out |
|---|---|---|
| ably-go | refused by default (RSC18) | ✅ `WithInsecureAllowBasicAuthWithoutTLS()` |
| ably-js | permissive (connects, warns) | n/a |
| ably-python | permissive (key over the WS query string worked, no token) | n/a |
| IO.Ably (.NET) | refused (`EnsureSecureConnection`) | ❌ none — the gap |

The PoC's portable answer for non-TLS loopback is **token auth** (short-lived
tokens are allowed over HTTP by all SDKs; the embedded server issues them at
`POST /keys/{keyName}/requestToken`). Recommendation: **IO.Ably should add a
dev-only opt-out matching ably-go's** — worth raising with the .NET SDK team.
Realtime is unaffected on all SDKs; this is a REST-over-plain-HTTP concern.

## Reliability / fault-injection (run, not described)

| Case | Go in-proc | Node (Express & Fastify) | .NET (YARP) | Python (FastAPI) |
|---|---|---|---|---|
| `kill -9` embedded child → auto-restart, harness passes | N/A (no child) | **PASS** (restart logged, re-ready, harness passes) | **PASS** | **PASS** |
| SIGTERM host app → clean exit, no orphan child | **PASS** (exit 0) | **PASS** (exit 0, ~0.1 s, no orphan) | **PASS** (exit 0, no orphan) | **PASS** (exit 0, no orphan) |
| resume-after-drop through the host | **PASS** | **PASS** | **PASS** | **PASS** |
| soak: 200 msgs, zero loss | **PASS** | **PASS** | **PASS** | **PASS** |

**Durability (disk mode).** In disk mode the state is a bbolt file, so it
survives a restart. Verified end to end: publish 3 messages, `kill -9` the
server, restart on the same data dir, all 3 are still in history (memory mode
shows 0). Proof: `results/disk-durability.log`, via the Go embed's
`Mode: "disk"`. The child-process tracks (Node/.NET/Python) pass `mode` +
`data-dir` through to the same storage layer.

The in-process ceiling has **no child to restart** — its trade-off is a
**shared blast radius** (a server panic takes the host app down). The
two-process tracks isolate the server crash and recover from it; that
isolation is the main reason to prefer the child-process model over
in-process for anything but Go-native, single-tenant use.

## Python and Java feasibility (assessment, not implemented)

The integration model — supervise a child binary, reverse-proxy WebSocket +
REST on a dedicated port — is language-agnostic. The only real variable is
**in-process WebSocket reverse-proxy maturity**.

- **Python** — *feasible.* Supervising the child via `subprocess`/`asyncio`
  is trivial; `/readyz` polling is a few lines. HTTP reverse-proxy under
  ASGI (Starlette/FastAPI + uvicorn) is straightforward with `httpx`. The
  cost is WebSocket proxying: bridging client↔upstream WS needs
  `websockets`/`httpx-ws` glue, and many teams would instead front the
  embed with nginx/Traefik rather than proxy WS in-process. Packaging: PyPI
  **platform wheels** carry the binary (the `ruff` precedent). Verdict:
  works; WS proxy is the integration cost, so Python is "assess, not
  primary" — exactly as the brief predicted.
- **Java** — *feasible, heavier.* Supervise via `ProcessBuilder`. In-process
  WS reverse-proxy is less idiomatic than Node/.NET but well supported by
  **Spring Cloud Gateway** (reactive, first-class WebSocket routes) or an
  Undertow/Netty proxy handler. Packaging: bundle per-OS natives in a JAR
  and extract at runtime (the `sqlite-jdbc` / `netty-tcnative` precedent).
  Verdict: works; Spring Cloud Gateway gives a clean WS proxy; more
  ceremony than Node/.NET.

Neither was implemented (time-boxed per the brief); nothing in the model
suggests either is blocked.

## Recommendation

**Embedded distribution is worth productising.** Per ecosystem:

- **Go — ship it.** Lowest glue (~3 lines), lowest overhead, no packaging
  problem (it's a normal dependency). Best for Go hosts that accept a shared
  blast radius (dev, CI, single-tenant self-host). The in-process handler is
  the natural Go-native distribution.
- **Node — ship it (primary).** Ergonomic (Express middleware / Fastify
  plugin), crash-isolated, ~8 lines of glue. Requires the two server
  fixes (now done) for ably-js. The real productisation work is **per-os/cpu
  binary packaging** via `optionalDependencies` (the esbuild model) plus
  macOS notarisation / Windows signing.
- **.NET — ship it, close behind.** YARP is the cleanest in-process WS proxy
  of the set. Realtime works unmodified; document the IO.Ably
  **key-auth-over-HTTP** limitation (use TLS or token auth). Packaging via
  NuGet `runtimes/<rid>/native`.
- **Python — ship it for ASGI.** FastAPI/Starlette (any ASGI host) proxies
  WS + REST cleanly; ably-python realtime works unmodified. **WSGI (Flask,
  sync Django) cannot proxy the WS in-process** — REST-only, or front it with
  an ASGI server / ingress. Packaging via PyPI platform wheels (the ruff
  model).

**What must change beyond this PoC:**
1. Promote the two server fixes to mainline (TASK-68 `connectionDetails`,
   TASK-69 `msgSerial` pointer) — they fix ably-js for *all* users, not just
   the embed.
2. Add an opt-in **`basePath`** option to the SDKs to unlock same-origin
   `/ably` mounting (§6, deferred) — removes the dedicated-port requirement.
3. Decide the binary-distribution/signing story per registry (npm, NuGet,
   PyPI) — the one genuine cost, not a technical blocker.
4. Single embedded node = no shared state across host instances; multi-
   instance needs `cluster` mode (shared Postgres). The embed is explicitly
   single-node; this is the scale path, not a PoC gap.

## Prior art — is this a hack, or precedent?

Spawning and supervising a child binary and talking to it over localhost is a
**mainstream, well-trodden pattern**, not a smell. It shows up in three
established lineages:

- **Ship a prebuilt native binary inside a language package** — esbuild, ruff,
  swc, Biome, Prisma, Playwright (browsers), `sharp`, tree-sitter, Tailwind
  standalone, `sentry-cli`, and Cloudflare's own `wrangler` (which bundles the
  `workerd` runtime). Utterly normal in 2026.
- **Spawn + supervise a real server process you talk to over localhost** —
  `mongodb-memory-server`, `embedded-postgres` / `pg-embed`,
  `redis-memory-server` (download + run + supervise + tear down a *real*
  server binary); **Prisma**'s Node ORM supervising its Rust `query-engine`
  binary for the app's lifetime; Playwright / Puppeteer / ChromeDriver /
  Selenium (spawn a browser binary, talk CDP/WebDriver over a local port);
  Testcontainers (spin up real dependencies, tear down); **dapr** (the sidecar
  pattern productised — a companion process over localhost); local emulators
  (Firebase Emulator Suite, Stripe CLI, LocalStack, MinIO, Azurite).
- **Reverse-proxy to a colocated upstream** — Vite / CRA / Next dev-server
  proxies, nginx, Caddy, YARP. Bread-and-butter.

The honest counterpoint: the reactions people *dislike* are specific, and we
can design them out — (1) **install-time binary downloads** (the
`mongodb-memory-server` CI-flake / supply-chain reputation) → ship prebuilt
platform packages instead, not postinstall downloads; (2) **orphaned/zombie
children and shutdown ordering** (we hit and fixed exactly this) → the library
owns supervision; (3) **extra process + deploy weight** → the Go in-process
path removes it. The most instructive precedent is **Prisma**: it shipped this
exact model at huge scale *and* invested heavily to move its engine
**in-process** (Node-API, then a WASM/Rust-free direction) to cut cold-start
and serverless friction — which both proves the pattern is legitimate and
validates why our **Go in-process track is the ceiling**. Executed well
(prebuilt packages, clean supervision, explicit single-instance/serverless
boundaries, an in-process option where the language allows), developers who
already use esbuild / Prisma / Playwright / Testcontainers will find this
familiar, not smelly.

## Where it does NOT fit (decision boundary)

Embedding assumes one long-lived process that owns a port and holds
connections open. That rules out, by architecture (not by polish):

- **Serverless / FaaS / edge** (Lambda, Cloudflare Workers, Deno Deploy, GCF,
  Azure Functions, Netlify Functions) — request-scoped, scale-to-zero, no
  long-lived process to embed into. *Nuance:* Vercel Functions (Fluid compute)
  now support WebSockets + Socket.IO and can even bundle/spawn binaries, so WS
  is no longer the blocker there — but the embed still doesn't fit because
  instances are duration-capped (WS drops at 800s/1800s), autoscaled, and each
  holds its own isolated `memory`-mode node (Vercel says use an external store
  for shared state). Point the SDK at a long-lived ably-server / cloud instead.
  Detail in [USING.md](USING.md#a-note-on-vercel-and-netlify-the-websocket-question).
- **Horizontally scaled multi-instance, in `memory` mode** — replicas don't
  share channels. Not a dead end: `ably-server`'s `cluster` mode (stateless
  nodes in front of a shared Postgres) gives real cross-instance fan-out — but
  running Postgres + orchestrating replicas leaves the "just a package"
  sweet spot, and that's the point to ask "why not the managed cloud?" The
  embed shines single-instance; shared-state scale is the upgrade path.
- **No-exec / locked-down filesystems** for the child-process tracks (Go
  in-process is exempt — nothing is spawned).
- **WSGI Python** (Flask, sync Django) for the in-process WS proxy — WSGI
  can't carry WebSockets; use ASGI or an ingress.

Friction (works, but plan for it): a WS-unaware ingress/LB with short idle
timeouts; single-port platforms (Heroku) vs. the dedicated-port model;
Alpine/musl (ship a static `CGO_ENABLED=0` binary); Windows child
supervision; per-OS/arch binary distribution + signing; `memory` mode is
ephemeral; Go in-process shares the host's crash blast radius.

**Sweet spot:** local dev, CI, single-tenant / self-hosted single-instance
apps, desktop/CLI bundling realtime, demos, on-prem single box. Full
per-language fit + friction detail in [USING.md](USING.md#when-it-fits--and-when-not-to-embed).

## What we haven't addressed (and how to close it)

Deliberately out of scope for the PoC, in rough priority order. Each is a
labelled follow-up with the concrete path to close it, so nothing here is a
surprise later:

1. **Observability / a control-plane API.** There is no `/metrics`, pprof,
   OTEL, or stats/admin/live-view endpoint today (only `/healthz` + `/readyz`).
   *Close:* a read-only `GET /stats` (active connections, attached channels,
   message counters) as the cheap DX win first; then Prometheus `/metrics`
   (TASK-27), pprof behind `--debug-listen` (TASK-40), OTEL via `OTEL_*`
   (TASK-41). Comparable tools ship this (Socket.IO Admin UI, Centrifugo,
   Temporal dev UI).
2. **Cross-compile matrix + real packaging.** Cross-compilation works (pure
   Go, static `CGO_ENABLED=0`) but every build here is host-arch and packaging
   is described, not implemented. *Close:* a `build-matrix.sh` over the six
   GOOS/GOARCH pairs; one conformance run against a linux/amd64 (and Alpine/
   musl) container; then per-registry packaging (npm `optionalDependencies`,
   NuGet `runtimes/<rid>`, PyPI wheels) with a thin end-to-end proof; then
   macOS notarisation / Windows signing.
3. **Cluster (Postgres) shared-state, run for real.** Disk-mode single-node
   durability is now proven (`results/disk-durability.log`); cluster mode is
   not exercised. *Close:* docker-compose Postgres + two host instances on the
   shared DSN, harness cross-instance fan-out (the server already has cluster
   integration tests to lean on).
4. **Windows graceful stop.** Node and Python send SIGTERM, which Windows
   ignores (hard kill, no drain); only .NET branches correctly. Nothing was
   run on Windows. *Close:* OS-branch the Node/Python stop (control-event /
   pipe) and run one Windows track. Today the docs label this precisely.
5. **Single-call convenience + package naming.** The public noun is now
   unified: Node, Python and .NET all expose `AblyServer` as the object a
   developer holds (`AblyServerSupervisor` remains a back-compat alias; the
   Node/Python proxy adapters accept `{ server }` and still tolerate
   `{ supervisor }`); Go intentionally has no noun (it's an `http.Handler`).
   What's left is ergonomic, not naming: `.NET`/Go read as "start and attach",
   while Node/Python still wire the server + proxy + `upgrade` by hand. *Close:*
   `attachAblyServer(app)` (Express) and a `mount_ably(app)` ASGI helper
   (Python). Separately, the class is `AblyServer` but the package is still
   `@ably/embedded-server` / module `ably_embedded` — decide the final product
   name (`@ably/server` vs the inherited `@ably/embedded-server`) so the class
   and package tell one story.
6. **Machine-readable ready-line.** Supervisors poll `/readyz` and pick a free
   port (small TOCTOU window). *Close:* emit a ready-line on stdout from
   `cmd/ably-server` after `net.Listen`; supervisors parse it (deferred per the
   brief).
7. **Test-harness robustness.** The fault-injection scripts identify the child
   via a machine-wide `pgrep`, which false-positives under concurrency (it did
   in a batch run; each case passes in isolation). *Close:* scope the match to
   the track's binary path / port.
8. **Security hardening.** Embedding co-hosts a token-issuing, publish-capable
   endpoint on your origin. The demo now carries a dev-only key banner; a
   production guide (token auth via `authUrl`, TLS off loopback, treat as a
   mounted admin surface) is still to write.

## Reproduce

```sh
# from the repo root, on this branch
go build -o experiments/embedding/harness/harness ./experiments/embedding/harness

# Floor
go build -o /tmp/ably-server ./cmd/ably-server
ABLY_SERVER_API_KEY=app.key:secret /tmp/ably-server --mode memory --listen 127.0.0.1:8431 &
experiments/embedding/harness/harness --port 8431 --label baseline --json

# Go in-process (ceiling)
go build -o /tmp/m1 ./experiments/embedding/go/example
ABLY_SERVER_API_KEY=app.key:secret /tmp/m1 --listen 127.0.0.1:8500 &
experiments/embedding/harness/harness --port 8500 --label go-inproc --json

# Node — see node/README.md ;  .NET — see dotnet/scripts/run-conformance.sh
```
