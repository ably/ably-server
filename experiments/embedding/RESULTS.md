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
1.2.18, ably-python 3.1.2. `memory` mode (zero external dependencies). All
loopback.

## The trade-off matrix (§7)

| Dimension | Floor (standalone) | Go in-process (ceiling) | Node (Express / Fastify) | .NET (YARP) | Python (FastAPI) |
|---|---|---|---|---|---|
| **Dev efficiency** — steps to first message | run 1 binary | **3** (import → mount handler → point SDK) | **4** (`npm i` → binary → ~8 lines → point SDK) | **3** from prebuilt (`dotnet build` → run → point SDK); ~1.6 s warm | **4** (venv+pip → binary → ~20 lines → point SDK) |
| **DX** — integration glue | n/a | **~3 lines** (`New` + mount `Handler` + `Close`) | **~8 lines** (supervisor + proxy + WS `upgrade` wiring) | **~20 lines** (host builder + YARP route + supervisor) | **~20 lines** (lifespan supervisor + `make_proxy_app` + ASGI fall-through) |
| **DX** — new concepts | n/a | ~1 (it's an `http.Handler`) | ~2 (child process; wire WS `upgrade`) | ~2 (child process; YARP cluster destination) | ~2 (child process; ASGI fall-through routing) |
| **DX** — idiomatic fit | n/a | native `net/http` | Express middleware / Fastify plugin | ASP.NET + YARP config | FastAPI/Starlette ASGI (WSGI can't do WS) |
| **Portability** — binary | 15.5 MB standalone | **+9.1 MB compiled into the host binary**, any Go target, no extra toolchain | 15.5 MB prebuilt **per os/cpu**, no toolchain on user machine | 15.5 MB prebuilt + **.NET runtime**; NuGet `runtimes/<rid>` | 15.5 MB prebuilt + **Python runtime**; PyPI platform wheels |
| **Ease of use** — auto free-port | — | host owns the port | **yes** (supervisor; proxy reads it live) | **yes** (supervisor; injected into YARP) | **yes** (supervisor; proxy reads it live) |
| **Ease of use** — auto-shutdown with app | — | inherent (`defer Close`) | yes (SIGTERM handler) | yes (host lifetime) | yes (ASGI lifespan) |
| **Reliability** — harness 6/6 | **PASS** | **PASS** | **PASS / PASS** | **PASS** | **PASS** |
| **Reliability** — server crash → auto-restart | n/a | **N/A** (no child) | **yes** (kill -9 → respawn → harness passes) | **yes** (kill -9 → respawn → harness passes) | **yes** (kill -9 → respawn → harness passes) |
| **Reliability** — graceful shutdown | n/a | clean (exit 0) | clean (exit 0, ~0.1 s, no orphan) | clean (exit 0, no orphan) | clean (exit 0, no orphan) |
| **Complexity** — processes | 1 | **1** | 2 | 2 | 2 |
| **Complexity** — failure blast radius | n/a | **shared** (in-process) | **isolated** (child) | **isolated** (child) | **isolated** (child) |
| **Native SDK through host** | — | ably-go | **ably-js ✓** (after the 2 server fixes) | **IO.Ably realtime ✓**; key-auth REST-over-HTTP ✗ | **ably-python realtime ✓** (key auth, no token) |

### Measured conformance (all tracks, same harness)

| Scenario | Floor | Go in-proc | Node Express | Node Fastify | .NET YARP | Python FastAPI |
|---|---|---|---|---|---|---|
| connect (ms) | 1.98 | 1.63 | 4.17 | 4.20 | 4.98–10.2 | 7.34 |
| pubsub mean / p99 (ms) | 0.14 / 0.18 | 0.11 / 0.16 | 0.15 / 0.18 | 0.19 / 0.28 | 0.16–0.26 / 0.27–0.42 | 0.37 / 0.47 |
| restpubsub mean (ms) | 0.32 | 0.29 | 0.67 | 1.97 | 0.47–0.64 | 1.96 |
| history (10, ordered) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| resume (RESUMED, lossless) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| soak (200 msgs, loss / dupes) | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| soak throughput (msg/s) | ~2037 | ~2247 | ~1005 | ~542 | ~1800–2150 | ~1114 |

Reading the numbers: the embedding **proxy overhead is small** — single-digit
millisecond connects and sub-millisecond pub/sub round-trips even through a
reverse proxy. The in-process ceiling is the fastest (no socket hop on the
host side); Node Fastify's REST path is the slowest because `@fastify/http-proxy`
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

| Case | Go in-proc | Node (Express & Fastify) | .NET (YARP) |
|---|---|---|---|
| `kill -9` embedded child → auto-restart, harness passes | N/A (no child) | **PASS** (restart logged, re-ready, harness passes) | **PASS** |
| SIGTERM host app → clean exit, no orphan child | **PASS** (exit 0) | **PASS** (exit 0, ~0.1 s, no orphan) | **PASS** (exit 0, no orphan) |
| resume-after-drop through the host | **PASS** | **PASS** | **PASS** |
| soak: 200 msgs, zero loss | **PASS** | **PASS** | **PASS** |

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

- **Serverless / FaaS / edge** (Lambda, Vercel/Netlify Functions, Cloudflare
  Workers, Deno Deploy, GCF, Azure Functions) — request-scoped, scale-to-zero,
  no persistent WS, usually no child-binary spawn. Nothing to embed into.
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
