# Embedding ably-server in other-language applications: a PoC brief

> **Status:** Self-contained execution brief. Branch `embedding-poc`. No
> implementation has landed yet. This document is the **single source of
> truth** for the experiment — an agent started inside this repo and pointed
> here has everything it needs to run it end to end. Read
> [§8 Execution guide and milestones](#8-execution-guide-and-milestones)
> first. See [DESIGN.md](DESIGN.md) for the server's design and
> [README.md](README.md) for what it is.
>
> **Routing decision (locked):** the PoC catches the standard, well-known
> Ably endpoints on a **dedicated port** and makes **no SDK change**.
> Mounting under a subpath namespace such as `/ably` is **deferred** until
> the Ably SDKs support a base path. See
> [§6 Routing decision](#6-routing-decision-locked).

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
(`--listen`, `/healthz` + `/readyz`, graceful shutdown), it can be embedded
into Node, a second managed-runtime language, and Go with **low integration
cost**, and the realtime transports keep working because traffic flows over
a **real local socket**, not a foreign-function call.

## 3. Why this matters

`ably-server`'s stated reason to exist is "drop-in Ably for local dev, CI,
and self-hosting: the SDK doesn't change, only the endpoint does"
([README.md](README.md)). Embedding is the strongest form of that promise:
if it is genuinely low-friction, `ably-server` becomes *installable realtime
infrastructure* ("install a package and you have an Ably endpoint") — a real
distribution wedge and a concrete data point for the wider build-vs-adapt
question. The point of this PoC is to find out how good or bad that
experience actually is, per ecosystem, before betting on it.

## 4. Scope

### In scope

- **Languages:** **Node** (primary), **.NET** (second — it has the
  strongest in-process WebSocket reverse-proxy story, via YARP), and **Go**
  (native, no binding — the upper bound on DX). **Python** and **Java** get
  a written feasibility assessment, with implementation only if time allows.
- **Integration model:** embedded **sidecar binary** supervised by the host
  process, with the host reverse-proxying realtime traffic to it. Use
  `memory` mode (zero external dependencies) for the PoC.
- **Unmodified SDKs against a dedicated port.** No SDK changes. The host
  points its Ably SDK at the embedded server by host + port.
- **Functional proof:** an unmodified Ably SDK (`ably-js` for Node,
  `ably-go` for Go, the Ably .NET SDK for .NET) connects *through the host
  app* to the embedded server and performs pub/sub, history, and
  resume-after-drop.

### Out of scope

- Any SDK change, and any subpath / namespace mounting (deferred — see §6).
- Production availability, multi-region, and `cluster`-mode scale testing
  (the PoC runs a single embedded node per app, in `memory` mode).
- Presence, push, Spaces, Chat, LiveObjects (already out of `ably-server`
  scope per [DESIGN.md §1](DESIGN.md)).
- Publishing real packages to public registries. We build and validate
  locally; publishing, signing, and notarisation are a later decision (but
  see Risks).

## 5. The integration model (and why not FFI)

Ship the **prebuilt binary inside the language package** and supervise it
from the host process. **No change to `ably-server` is required** — the PoC
embeds the binary as built:

1. **Resolve + spawn** the right per-platform binary on startup.
2. **Discover its port:** the supervisor picks a free TCP port and passes
   `--listen 127.0.0.1:<port>`, then waits for `/readyz` to go green. (See
   "deferred" below for a more robust ready-signal option.)
3. **Reverse-proxy** the well-known endpoints (or a dedicated port) to it,
   with WebSocket upgrade support.
4. **Supervise:** restart on crash; shut down gracefully with the app
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

## 6. Routing decision (locked)

**Decision:** the host app routes the **standard, well-known Ably endpoints**
to the embedded server on a **dedicated port** (or subdomain), and the PoC
makes **no change to the Ably SDKs**.

**Why.** Confirmed against `ably-js` 2.22.1 source: the SDK builds
root-rooted URLs and has **no base-path option**. REST is
`https://<host>:<port>/...` (`baseclient.ts` `baseUri`), WebSocket connects
to `wss://<host>:<port>/` (`websockettransport.ts`), and comet uses a
hardcoded `/comet/...` (`comettransport.ts`). The configurable knobs are
`endpoint` / `restHost` / `realtimeHost` / `port` / `tlsPort` only. A
same-origin subpath is therefore impossible with a stock SDK today, and
forcing it would mean shipping a forked SDK — which defeats the "unmodified
SDK" point of the whole exercise.

So the host points its SDK at the embedded server by host + port, e.g.:

```js
// ably-js
new Ably.Realtime({
  key,
  restHost: '127.0.0.1', realtimeHost: '127.0.0.1',
  port: embeddedPort, tls: false,
});
```

**Deferred (future, explicitly NOT in this PoC):** add an opt-in `basePath`
option to the SDKs — a small, additive change threading it through the three
URI builders plus `ClientOptions` (~3 call sites + 1 type per SDK) — then
mount the server under a namespace such as `/ably` on the host's main
origin. The server side is already trivial (wrap the mux in
`http.StripPrefix("/ably", …)`). We prove the model first with unmodified
SDKs; the subpath ergonomics come later, SDK-side.

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
  `ably-server` binary. Isolates the *embedding* cost from the server itself.
- **Ceiling (best case):** Go in-process mount. Shows the lower bound on glue
  and overhead that the managed-runtime languages are measured against.

## 8. Execution guide and milestones

**This brief is self-contained.** An agent started inside this repo and
pointed at this file needs no prior conversation. Work on the
`embedding-poc` branch.

### Prerequisites (toolchains the session needs)

- **Go 1.25+** — build/run `ably-server`; the Go track.
- **Node 20+** and a package manager (npm/pnpm) — the Node track.
- **.NET SDK 8+** — the .NET track.
- **Network access** for npm/NuGet and to fetch the public Ably SDKs
  (`ably-js`, Ably .NET, `ably-go`).
- **No database, no external services** (the PoC uses `memory` mode).
- Optional: the **Backlog.md CLI** (`backlog`) to track tasks the repo's
  way; install with `npm i -g backlog.md` if absent, or execute the
  milestones directly and track via commits.

### Working layout

Put all PoC code under `experiments/embedding/` so the tracks do not
collide (this matters for running them in parallel):

```
experiments/embedding/
  harness/   # language-agnostic conformance scenarios (pub/sub, history, resume) + runner
  go/        # M1 Go in-process embed (inside this module — can import internal/)
  node/      # M2 Node package + example app
  dotnet/    # M3 .NET + YARP package + example app
  RESULTS.md # M4 output: the filled trade-off matrix + recommendation
```

Add a `.gitignore` for `node_modules/`, `bin/`, `obj/`, and any downloaded
binaries. The Go track lives inside the module (`github.com/ably/ably-server`)
so it can import `internal/...`; keep it from perturbing the main module's
`go test ./...` (build tag or a clearly separate package).

### Kickoff

The repo tracks work with Backlog.md ([backlog/AGENT_GUIDELINES.md](backlog/AGENT_GUIDELINES.md)).
First action: create the milestone tasks via the CLI (commands in §11). Set
each task In Progress and assign it when you start; check acceptance
criteria as you go; add a Final Summary when done. If the CLI is
unavailable, proceed and track progress in atomic per-milestone commits.

### Dependency graph and parallelization

```
M0  harness + baseline ──┬──►  M1  Go embed   ─┐
   (only hard prereq)    ├──►  M2  Node        ─┼──►  M4  measure + write up
                         └──►  M3  .NET + YARP ─┘     (barrier: needs all 3)
```

**M0 is the only hard prerequisite.** M1, M2 and M3 are independent (separate
directories, separate stacks) and **should run in parallel** — one subagent
or git worktree per track. M4 is the join point: it needs all three tracks'
measured numbers.

### Milestones

- **M0 — Harness + baseline.** Build a conformance harness (publish,
  subscribe, history, resume-after-drop) runnable against any endpoint by
  host+port. Run it against a plain binary to set the floor:
  `ABLY_SERVER_API_KEY=app.key:secret go run ./cmd/ably-server --mode memory --listen :8080`.
  **Confirm transport coverage first:** the server exposes WebSocket at
  `GET /` and REST; verify whether a non-WebSocket fallback (comet/SSE)
  exists. If not, scope the cross-transport reliability leg to
  WebSocket + REST (and optionally raise a server task). No server change is
  needed: the supervisor picks a free port and passes `--listen`, then polls
  `/readyz`.
- **M1 — Go in-process embed (ceiling; parallel).** In
  `experiments/embedding/go`, a tiny Go app that wires the same
  manager/handlers as [cmd/ably-server/main.go](cmd/ably-server/main.go) and
  mounts them **in-process** (no child process), then runs the harness.
  Establishes the DX/overhead lower bound.
- **M2 — Node package (primary; parallel).** In `experiments/embedding/node`,
  a local package that resolves + spawns the prebuilt binary (free port +
  `--listen`, health-check `/readyz`, restart-on-crash, shutdown with the
  app), plus **Express and Fastify** middleware that reverse-proxy to it with
  WebSocket support. Example app + harness run + recorded metrics.
- **M3 — .NET + YARP (second; parallel).** In `experiments/embedding/dotnet`,
  a local package, `Process` supervision, and a **YARP** route with
  WebSocket proxying. Example app + harness run + recorded metrics.
- **M4 — Measure + write up (barrier).** Fill the §7 trade-off matrix with
  **measured** numbers for Go, Node and .NET in
  `experiments/embedding/RESULTS.md`; deliver a clear recommendation.

**Deferred (not in this PoC):** a machine-readable ready-line on stdout for
more robust port discovery; and the `basePath` SDK feature + same-origin
`/ably` namespace (§6).

### Quality bar

- Each track's harness run must **actually pass** (pub/sub + history + resume
  through the host app) — capture real output; do not assume or fake.
- Record **measured** numbers, not estimates.
- Fault-injection (`kill -9`, `SIGTERM`, mid-stream drop) must be **run**,
  not described.
- Atomic commit per milestone. Do not push to `ably/server` or open a PR
  without the owner's go-ahead.

## 9. Success criteria

- The harness passes (pub/sub + history + resume) **through the host app** in
  Node, .NET and Go, over WebSocket (and a non-WebSocket path where the
  server supports one).
- The embedded server survives `kill -9` and is auto-restarted; the host app
  survives a server crash; graceful shutdown is clean.
- `RESULTS.md` contains the §7 matrix filled with **measured** numbers for
  all six dimensions across the three languages.
- A clear recommendation: is embedded distribution worth productising, for
  which ecosystems, and what (if anything) must change in `ably-server` or
  the SDKs later (notably `basePath` for same-origin mounting).

## 10. Risks and open questions

- **Binary distribution overhead.** Go binaries are ~10–20 MB; per-platform
  packages multiply registry footprint. macOS Gatekeeper notarisation and
  Windows code-signing are real productisation costs. Measure size; flag
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

## 11. Kickoff tasks (Backlog.md)

The Backlog.md CLI is the source of truth for tasks
([backlog/AGENT_GUIDELINES.md](backlog/AGENT_GUIDELINES.md)). Create these at
kickoff (titles indicative; M1–M3 can be worked in parallel):

```sh
backlog task create "Embedding conformance harness (pub/sub, history, resume) runnable against any host:port" \
  -d "Language-agnostic scenarios + runner. Baseline run against a standalone ably-server binary." \
  --ac "harness drives publish, subscribe, history and resume-after-drop" \
  --ac "passes against a plain ably-server --mode memory binary (baseline numbers recorded)" \
  --ac "confirms whether a non-WebSocket transport exists; scopes reliability leg accordingly"
backlog task create "M1: Go in-process embed example + harness run (DX ceiling)" \
  --ac "ably-server handlers mounted in-process in a tiny Go app (no child process)" \
  --ac "harness passes through it; metrics recorded"
backlog task create "M2: Node embedded-server package: binary resolve+supervise + Express/Fastify WS proxy" \
  --ac "spawns binary on a free port, health-checks /readyz, restarts on crash, shuts down with app" \
  --ac "Express and Fastify middleware proxy realtime traffic incl. WebSocket" \
  --ac "harness passes through an example Node app; metrics recorded"
backlog task create "M3: .NET + YARP embedded-server package + harness run" \
  --ac "Process supervision + YARP route with WebSocket proxying" \
  --ac "harness passes through an example .NET app; metrics recorded"
backlog task create "M4: Fill the embedding trade-off matrix (RESULTS.md) + recommendation" \
  --ac "all six dimensions measured for Go, Node and .NET" \
  --ac "RESULTS.md states a clear go/no-go recommendation per ecosystem"
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
