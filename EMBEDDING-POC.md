# Embedding ably-server in other-language applications: a PoC brief

> **Status:** Draft brief. Branch `embedding-poc`. This document defines a
> proof of concept; no implementation has landed yet. See
> [DESIGN.md](DESIGN.md) for the server's own design and
> [README.md](README.md) for what it is.

## 1. Objective

Prove (or disprove) that `ably-server` can be shipped as a **dependency of a
non-Go web application**: installed with that ecosystem's own package
manager, supervised by the host app, and reached by **unmodified Ably
SDKs**, so a developer can add realtime to their existing app without
standing up separate infrastructure. Cover at least **two non-Go languages
plus Go**, and **measure** the developer-experience and operational
trade-offs rather than asserting them.

The shape a developer should end up with:

```
npm install @ably/embedded-server      # (or pip / dotnet add / go get)
# ...a few lines of glue in their app...
# their app now answers Ably-SDK traffic, backed by an embedded server
```

## 2. Hypothesis

Because `ably-server` is a single, dependency-free Go binary (the `memory`
and `disk` modes need no external service) with a clean lifecycle
(`--listen`, a ready-port signal, `/healthz` + `/readyz`, graceful
shutdown), it can be embedded into Node, a second managed-runtime language,
and Go with **low integration cost**, and the realtime transports keep
working because traffic flows over a **real local socket**, not a
foreign-function call.

## 3. Why this matters

`ably-server`'s stated reason to exist is "drop-in Ably for local dev, CI,
and self-hosting: the SDK doesn't change, only the endpoint does"
([README.md](README.md)). Embedding is the strongest form of that promise:
if it is genuinely low-friction, `ably-server` becomes *installable
realtime infrastructure* ("install a package and you have an Ably endpoint")
— a real distribution wedge and a concrete data point for the wider
build-vs-adapt question. The point of this PoC is to find out how good or
bad that experience actually is, per ecosystem, before betting on it.

## 4. Scope

### In scope

- **Languages:** **Node** (primary), **.NET** (second — it has the
  strongest in-process WebSocket reverse-proxy story, via YARP), and **Go**
  (native, no binding — the upper bound on DX). **Python** and **Java** get
  a written feasibility assessment, with implementation only if time allows.
- **Integration model:** embedded **sidecar binary** supervised by the host
  process, with the host reverse-proxying realtime traffic to it. Use
  `memory` mode (zero external dependencies) for the PoC.
- **Functional proof:** an unmodified Ably SDK (`ably-js` for Node,
  `ably-go` for Go, the Ably .NET SDK for .NET) connects *through the host
  app* to the embedded server and performs pub/sub, history, and
  resume-after-drop.

### Out of scope

- Production availability, multi-region, and `cluster`-mode scale testing
  (the PoC runs a single embedded node per app, in `memory` mode).
- Presence, push, Spaces, Chat, LiveObjects (already out of `ably-server`
  scope per [DESIGN.md §1](DESIGN.md)).
- Publishing real packages to public registries. We build and validate
  locally; publishing, signing, and notarisation are a later decision (but
  see Risks).

## 5. The integration model (and why not FFI)

Ship the **prebuilt binary inside the language package** and supervise it
from the host process:

1. **Resolve + spawn** the right per-platform binary on startup.
2. **Discover its port** from the ready signal (it already supports
   `--listen :0`; `run()` reports the bound address via `runOpts.Ready`,
   see [cmd/ably-server/main.go](cmd/ably-server/main.go)).
3. **Health-check** `/readyz` before routing traffic.
4. **Reverse-proxy** a chosen route (or a dedicated port) to it, with
   WebSocket upgrade support.
5. **Supervise:** restart on crash; shut down gracefully with the app
   (`--shutdown-grace`).

**Why a child process and not a Go FFI binding.** The realtime transports
hold the connection open: WebSocket hijacks the socket, and any HTTP
long-poll fallback holds the request. A foreign-function binding naturally
exposes `handle(request) -> response`, which cannot carry a long-lived or
streamed connection. The only in-process binding that *would* work is "Go
opens a loopback listener inside the host process", which still talks over a
socket while adding cgo, two runtimes in one process (two GCs, conflicting
signal handlers), and a shared crash blast-radius. Not worth it versus a
supervised child. WebAssembly is a non-starter for an inbound-WebSocket
server. **Go is the exception:** no binding at all — import the package and
mount the handler in-process.

**Packaging precedent** (mature, battle-tested): esbuild ships its Go binary
via npm `optionalDependencies` keyed by `os`/`cpu`; `ruff` ships a Rust
binary via PyPI platform wheels; `sqlite-jdbc` / `netty-tcnative` bundle
per-OS natives in a Maven JAR and extract at runtime; NuGet uses
`runtimes/<rid>/native/`.

## 6. The routing constraint (`/ably` on the same origin)

Confirmed against `ably-js` 2.22.1 source: the SDK builds **root-rooted
URLs** and has **no base-path option**. REST is `https://<host>:<port>/...`
(`baseclient.ts` `baseUri`), the WebSocket transport connects to
`wss://<host>:<port>/` (`websockettransport.ts`), and the comet fallback
uses a hardcoded `/comet/...` (`comettransport.ts`). The configurable knobs
are `endpoint` / `restHost` / `realtimeHost` / `port` / `tlsPort` /
`fallbackHosts` — host and port only.

So an **unmodified SDK cannot be pointed at `app.example.com/ably/...`**.
Options:

| Routing | Works with stock SDK? | How | Trade-off |
|---|---|---|---|
| **Dedicated port** | ✅ today | App on 443, server on e.g. 8443; SDK `{ host, port: 8443, tls }` | Still one deployable / one supervisor, just not one port |
| **Subdomain** | ✅ today | `realtime.example.com` → server; SDK `endpoint`/`*Host` | Cleanest for TLS/CDN; one DNS record |
| **Same-origin `/ably`** | ⚠️ needs small SDK feature | Server: wrap the mux in `http.StripPrefix("/ably", …)` (trivial; it switches on root paths). Client: add a `basePath` option | The only option needing a code change |

The server side of same-origin is a one-liner here. The client side is a
**small, additive** SDK feature: thread a `basePath` through the 3 URI
builders plus `ClientOptions` (~3 call sites + 1 type per SDK), opt-in and
non-breaking. **The PoC uses a dedicated port** by default; an optional
stretch milestone spikes `basePath` in `ably-js` to demonstrate true
same-origin `/ably` end-to-end and to size the SDK change for real.

## 7. What we measure: the trade-off framework

The whole point is the trade-offs. Six dimensions, each with concrete,
recorded metrics and a method — no hand-waving.

| Dimension | Metrics | Method |
|---|---|---|
| **Developer efficiency** (time-to-first-message) | Wall-clock minutes and step count from `install` to a working pub/sub round-trip, following only the package README | Timed, scripted run-through per language; record exact commands + minutes |
| **Developer experience (DX)** | Lines of integration glue; number of new concepts a dev must hold; does it fit idiomatic framework patterns (middleware); quality of error messages on misconfiguration | Count glue LOC; 1–5 rubric scored per language; capture the actual failure-mode output |
| **Platform portability** | OS/arch the prebuilt binary covers; does install+run work on macOS-arm64 and Linux-x64 (and Windows) with **no toolchain**; binary size | Build matrix; run the harness on each target (CI matrix + containers); record sizes |
| **Ease of use** | One-liner to start? Auto-port? Auto-shutdown with the app? Config surface (env vs flags) | Ergonomics checklist against the integration API |
| **Reliability** | App survives a server crash and restarts it; graceful shutdown is clean; SDK reconnects/resumes across a forced drop; soak (N min, M messages, zero loss) | Fault injection: `kill -9` the child, `SIGTERM` the app, drop the proxy mid-stream; assert SDK recovery and zero message loss |
| **Complexity / operational cost** | Moving parts added to a deployment; supervision burden; packaging complexity per ecosystem; failure blast-radius (child vs in-process) | Architecture inventory + a per-language "what can break" list |

**Bracket the spectrum with two baselines** so the numbers mean something:

- **Floor (do-nothing):** point the SDK straight at a standalone
  `ably-server` binary / container. Isolates the *embedding* cost from the
  server itself.
- **Ceiling (best case):** Go in-process mount. Shows the lower bound on
  glue and overhead that the managed-runtime languages are measured against.

## 8. Method and milestones

- **M0 — Harness + baselines.** A shared, language-agnostic conformance
  script (publish, subscribe, history, resume-after-drop) runnable against
  any endpoint. Run it against a plain `ably-server` binary to set the floor
  numbers. **Confirm transport coverage first:** the server today exposes
  WebSocket at `GET /` and REST; verify whether a non-WebSocket fallback
  (comet/SSE) exists, because the "mixed transport" reliability leg depends
  on it. If absent, either scope the reliability test to WebSocket + REST or
  raise a server task.
- **M1 — Embedding affordances.** Confirm/extend the hooks an embedder
  needs: a `--base-path` flag that wraps the mux in `http.StripPrefix`; a
  machine-readable ready signal a parent can read without scraping logs
  (e.g. the bound address as a JSON line on stdout, building on
  `runOpts.Ready`); confirm `/readyz` semantics. (Backlog tasks.)
- **M2 — Go in-process embed (ceiling).** Mount the handler under `/ably` in
  a tiny Go app; run the harness; record metrics.
- **M3 — Node package (primary).** A local `@ably/embedded-server`-style
  package: per-platform binary resolution, spawn/supervise, and an
  Express + Fastify middleware that proxies with WebSocket support. Run the
  harness through it; record metrics.
- **M4 — Second language: .NET + YARP.** A local NuGet-style package,
  `Process` supervision, a YARP route with WebSocket proxying and path
  transform. Run the harness; record metrics.
- **M5 — (stretch) `basePath` SDK spike.** Implement `basePath` in `ably-js`
  and demonstrate same-origin `/ably` end-to-end; size the change across the
  SDK family.
- **M6 — Measure + write up.** Fill the trade-off matrix with measured
  numbers for Node, .NET, and Go; deliver a recommendation.

## 9. Success criteria

- The harness passes (pub/sub + history + resume) **through the host app**
  in Node, .NET, and Go, over WebSocket (and a non-WebSocket path where the
  server supports one).
- The embedded server survives `kill -9` and is auto-restarted; the host app
  survives a server crash; graceful shutdown is clean.
- The trade-off matrix (§7) is filled with **measured** numbers, not
  estimates, for all six dimensions across the three languages.
- A clear recommendation: is embedded distribution worth productising, for
  which ecosystems, and what (if anything) must change in `ably-server` or
  the SDKs (notably `basePath`).

## 10. Risks and open questions

- **Same-origin `/ably` needs an SDK change.** Accept dedicated-port for the
  PoC, or invest in the `basePath` spike (M5)? Decision needed early.
- **Binary distribution overhead.** Go binaries are ~10–20 MB; per-platform
  packages multiply registry footprint. macOS Gatekeeper notarisation and
  code-signing on Windows are real productisation costs. Measure size; flag
  signing as out-of-PoC but on the cost ledger.
- **Single embedded node = no shared state.** Two app instances each embed
  their own `memory`-mode node and will not see each other's channels.
  Multi-instance needs `cluster` mode (shared Postgres). The PoC is
  explicitly single-node; note the scale path but do not test it.
- **Weaker proxy story in Python/Java.** In-process WebSocket reverse-proxy
  support is less mature than Node's or .NET's (often pushed to an ingress
  like nginx/Traefik). This is why they are assessed, not primary.
- **Transport parity.** If `ably-server` lacks a comet/SSE fallback, the
  cross-transport reliability claim is limited to WebSocket; confirm in M0.

## 11. Proposed Backlog.md tasks

The Backlog.md CLI is the source of truth for tasks (see
[backlog/AGENT_GUIDELINES.md](backlog/AGENT_GUIDELINES.md)); these are not
yet created because the CLI is not installed in this environment. Seed them
with (titles indicative):

```sh
backlog task create "Add --base-path flag mounting the mux under a path prefix (http.StripPrefix)" \
  -d "Let an embedder mount the server under e.g. /ably; switch on root paths after stripping." \
  --ac "GET /ably/ upgrades to WebSocket" --ac "REST under /ably/channels/... works" \
  --ac "default (no --base-path) behaviour unchanged"
backlog task create "Emit a machine-readable ready signal (bound addr as JSON on stdout)" \
  -d "A supervising parent process can read the ephemeral port without scraping logs; builds on runOpts.Ready."
backlog task create "Embedding conformance harness (publish/subscribe/history/resume) runnable against any endpoint" \
  --ac "passes against a standalone ably-server binary (baseline)"
backlog task create "Go in-process embed example + harness run (DX ceiling)"
backlog task create "Node embedded-server package: binary resolution + supervise + Express/Fastify WS proxy"
backlog task create "Second language (.NET + YARP) embedded-server package + harness run"
backlog task create "Fill the embedding trade-off matrix with measured numbers + recommendation"
```

## 12. References

- This repo: [README.md](README.md), [DESIGN.md](DESIGN.md) (§2 external
  surface, §5 internal architecture, §6 storage, §9 configuration, §11
  lifecycle), [cmd/ably-server/main.go](cmd/ably-server/main.go).
- SDK URL construction (`ably-js` 2.22.1): REST `baseUri` in
  `common/lib/client/baseclient.ts`; WebSocket in
  `common/lib/transport/websockettransport.ts`; comet in
  `common/lib/transport/comettransport.ts`. No base-path option in
  `ClientOptions`.
- Packaging precedent: esbuild (npm `optionalDependencies` per os/cpu),
  `ruff` (PyPI platform wheels), `sqlite-jdbc` / `netty-tcnative` (Maven
  natives), NuGet `runtimes/<rid>/native`.
