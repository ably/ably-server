# Using embedded ably-server: run it, use it, ship it

> How a developer in Go, Node, .NET, or Python adds a realtime Ably endpoint
> to their own web app by embedding `ably-server` — with no separate service
> to run and no client dependency to install. Companion to
> [RESULTS.md](RESULTS.md) (the measured trade-offs) and the live
> [browser demo](demo/index.html).

## Why this is good

You already run a web server. Embedding turns it into an Ably endpoint too:

- **No dependency to install** — the realtime server ships *inside* your app
  as a normal package dependency (or, in Go, a compiled-in import).
- **No separate server to run** — it's the same process (Go) or a child
  process your app supervises (Node/.NET/Python), on the same port. One thing
  to deploy, one thing to monitor.
- **Just an endpoint, attached** — point any unmodified Ably SDK at your
  host + port. Pub/sub, history, and resume-after-drop all work, because the
  traffic flows over a real local socket, not a foreign-function call.

Open the [demo](demo/index.html) (served by every example app at `/demo/`)
and you're watching live messages flow through a realtime endpoint your own
web server is hosting.

## Try it in ~30 seconds

Every track builds the same prebuilt `ably-server` binary once, then runs a
host app. "Run it" starts your app with Ably embedded; "try it" proves
realtime works against it (the cross-language Go conformance harness, or that
language's own SDK). Open `http://127.0.0.1:<port>/demo/` in two browser tabs
to see it live.

| Stack | Run it | Try it against it |
|---|---|---|
| **Go** (in-process) | `go run ./experiments/embedding/go/example --listen 127.0.0.1:8500 --demo-dir experiments/embedding/demo` | `go run ./experiments/embedding/harness --port 8500 --label go` |
| **Node** (Express) | `cd experiments/embedding/node && npm i && ABLY_PUBLIC_PORT=8541 node examples/express-app.js` | `node examples/ably-js-smoke.js` (ABLY_PUBLIC_PORT=8541) — unmodified ably-js |
| **Node** (Fastify) | `ABLY_PUBLIC_PORT=8542 node examples/fastify-app.js` | `experiments/embedding/harness/harness --port 8542 --label node` |
| **.NET** (YARP) | `cd experiments/embedding/dotnet && dotnet run --project examples/ExampleApp -- --port 8561` | `dotnet run --project examples/SdkSmoke -- --port 8561` — unmodified IO.Ably |
| **Python** (FastAPI) | `cd experiments/embedding/python && go build -o bin/ably-server ../../../cmd/ably-server && python3 -m venv .venv && ./.venv/bin/pip install fastapi "uvicorn[standard]" websockets httpx ably && ABLY_PUBLIC_PORT=8581 ./.venv/bin/uvicorn examples.fastapi_app:app --port 8581` | `ABLY_PUBLIC_PORT=8581 ./.venv/bin/python examples/ably_python_smoke.py` — unmodified ably-python |

(The harness binary is built once with
`go build -o experiments/embedding/harness/harness ./experiments/embedding/harness`.)

## The model

The host points its SDK at the embedded server by **host + port** on a
**dedicated port** (the whole port is the Ably endpoint). This is the routing
decision locked in [EMBEDDING-POC.md §6](../../EMBEDDING-POC.md): it works
with **stock SDKs today**, no SDK change.

```
            ┌────────────────────────── your host app ───────────────────────────┐
 Ably SDK ──▶  :8541  ──▶  reverse proxy (Node/.NET/Python)  ──▶  ably-server child │
 (host+port)                         (or, in Go, the handler is mounted in-process) │
            └────────────────────────────────────────────────────────────────────┘
```

Two shapes, by language:
- **Go — in-process.** No child, no proxy: import the package, mount the
  `http.Handler`. Lowest overhead; shares the app's process (a crash takes the
  app down). Best for Go hosts.
- **Node / .NET / Python — supervised child + reverse proxy.** The package
  spawns the prebuilt binary on a free loopback port, health-checks `/readyz`,
  restarts it on crash, stops it with the app, and reverse-proxies WebSocket +
  REST to it. The server crash is isolated from the host app.

## When it fits — and when NOT to embed

Embedding needs **one long-lived process that owns a port and holds
connections open** (or, for Go, that process is your app). That assumption is
what makes it shine for dev, CI, single-tenant and self-hosted apps — and
it's exactly what some environments break.

### Won't work (architectural mismatch — don't try)

- **Serverless / FaaS / edge runtimes** — AWS Lambda, Cloudflare Workers, Deno
  Deploy, Google Cloud Functions, Azure Functions, Netlify Functions. They're
  request-scoped, scale to zero, and (mostly) can't spawn an arbitrary child
  binary or hold a long-lived process to embed into. Point a *separately
  hosted* ably-server (or Ably cloud) at them instead — that's not embedding.
  See the Vercel note below: WebSockets are no longer the blocker there, but
  the embed still doesn't fit.
- **Horizontally scaled / multi-instance, in `memory` mode** — each instance
  is its own in-memory node, so two replicas (pods, dynos, a rolling deploy
  mid-cutover) are **two isolated Ablys**: a client on A never sees a publish
  on B. This is a `memory`-mode limit, *not* a dead end — `ably-server`'s
  `cluster` mode (stateless nodes in front of a shared Postgres) gives real
  cross-instance fan-out, and that's the design. But the moment you're running
  Postgres and orchestrating replicas, you've left the "just a package,
  nothing else" sweet spot. That's the boundary, and it's the honest place to
  ask "why not the managed cloud?" rather than scale a self-hosted,
  single-region server. The embed shines at single-instance; shared-state
  scale is the on-ramp's *upgrade*, not its job.
- **No-exec / locked-down filesystems** — App Engine standard, distroless
  images without a shell/exec, `noexec` mounts, strict SELinux/AppArmor. The
  child-process tracks can't spawn the binary. (Go in-process is the
  exception — nothing is spawned, so this restriction doesn't apply.)
- **WSGI Python** — Flask, classic synchronous Django, Bottle. WSGI has no
  WebSocket; it can reverse-proxy REST but not the realtime WS in-process.
  Use an ASGI stack (FastAPI / Starlette / async Django / aiohttp), or front
  the WS with an ingress.
- **Same-origin subpath with a stock SDK, today** — the SDKs have no
  `basePath` option yet (see the subpath section). Use a dedicated port or
  subdomain until that lands.

### A note on Vercel and Netlify (the WebSocket question)

Vercel **now supports WebSockets** in Functions (including `ws`, Socket.IO,
and Python), on [Fluid compute](https://vercel.com/docs/functions/websockets).
You could even bundle and spawn the binary
([large functions](https://vercel.com/docs/functions/limitations) allow 5 GB
bundles "for binaries"; Node.js has full `child_process` coverage). So "no
WebSockets" is no longer why serverless is out — that reason is stale.

But **embedding the sidecar still doesn't fit**, for the instance lifecycle:
a WebSocket "closes when a Function reaches its maximum duration" (800s GA,
1800s beta, 300s Hobby), so the spawned server and its in-memory state recycle
every ~13–30 minutes; instances autoscale and "new connections are not
guaranteed to reach the same instance" (Vercel's own advice is to keep shared
state in an external store like Redis), so each instance is an **isolated,
resetting `memory`-mode node**; and there's a **1,024 file-descriptor** ceiling
per instance. You'd get fragmented realtime unless you front it with a shared
Postgres — at which point the "just a package" value is gone. The fit on
Vercel is to use its native WebSockets for your app's own realtime, or point an
unmodified Ably SDK at a long-lived ably-server / Ably cloud. Neither is
embedding. **Netlify Functions**, by contrast, still don't hold persistent
WebSockets in 2026 — their guidance is to use a realtime provider.

### Friction points (it works, but mind these)

- **A WebSocket-unaware ingress / LB / CDN in front of you.** Your own load
  balancer or reverse proxy must forward the `Upgrade` header and not kill
  idle connections — many default to short idle timeouts that drop WS. This
  is a deployment-config issue, not an embedding one, but people hit it.
- **Single-port platforms** (Heroku's one `$PORT`, some PaaS). You get one
  port, so Ably takes that port's root (your app loses `/`) or you use a
  subdomain. Same-origin `/ably` needs the deferred SDK `basePath`.
- **Alpine / musl containers.** Ship a **static** binary (`CGO_ENABLED=0` —
  ably-server is pure Go, so this just works); a glibc-linked binary won't
  run on musl. The binary must also match the container's OS/arch.
- **Windows child supervision.** No POSIX `SIGTERM`; graceful shutdown of the
  child needs Windows-specific signalling. The Go in-process path avoids it.
- **Binary distribution cost.** Per-OS/arch binaries multiply package size;
  macOS notarisation and Windows code-signing are real productisation costs.
- **`memory` mode is ephemeral.** A restart loses channel state and history.
  Fine for dev/CI; for durability use `disk` (single node) or `cluster`.
- **Go in-process shares the blast radius.** A panic in the server takes the
  host app down. The child-process tracks (Node/.NET/Python) isolate a crash
  and auto-restart it — that's the main reason to prefer them over Go
  in-process for anything but Go-native, single-instance use.

### Where it works *well*, per language

| Language | Sweet spot | Watch out for |
|---|---|---|
| **Go** | Go services, CLIs, desktop apps, single-binary distribution — in-process, zero glue, lowest overhead, no binary to ship | shared blast radius (no crash isolation) |
| **Node** | dev servers, single-instance Express/Fastify apps, Electron apps, local tooling — idiomatic middleware, crash-isolated child | per-os/cpu binary packaging; remember to wire the WS `upgrade` (Express) |
| **.NET** | single-instance ASP.NET apps, desktop hosts, on-prem — YARP is the cleanest WS proxy of the set | IO.Ably refuses key auth over plain HTTP (use token auth); .NET runtime required |
| **Python** | FastAPI / Starlette dev servers, local AI apps (ASGI) — clean async proxy | **WSGI (Flask/sync Django) can't proxy WS in-process**; check ably-python realtime support |

### The one-line rule

**Embed for: local dev, CI/test, single-tenant or self-hosted single-instance
apps, desktop/CLI apps that bundle realtime, demos, on-prem-on-one-box.**
**Don't embed for: serverless/edge, or multi-replica production realtime that
needs shared durable state** — there, use Ably's platform (or `cluster` mode
if you're self-hosting at scale).

## Per-language: dev/local now, production later

In the PoC you **build the binary locally and link it** (the dev/local path
below). In production you'd ship it as a **platform package** so the host dev
never touches Go — the precedent is esbuild (npm), ruff (PyPI), and
sqlite-jdbc/netty-tcnative (Maven natives) / NuGet `runtimes/<rid>`.

Build the binary once (dev/local):
```sh
go build -o experiments/embedding/<track>/bin/ably-server ./cmd/ably-server
```

### Go — `http.Handler`, no binary at all

In Go there is no binary to ship — you import the package and the server is
compiled into your app. It's an `http.Handler`, so it drops into any router.

```go
import "github.com/ably/ably-server/experiments/embedding/go/ablyembed"

embedded, _ := ablyembed.New(ablyembed.Options{APIKey: "app.key:secret"})
defer embedded.Close()

// net/http
http.ListenAndServe("127.0.0.1:8500", embedded.Handler)

// chi / gin / echo — it's just an http.Handler:
//   r := chi.NewRouter(); r.Mount("/", embedded.Handler)
//   r.Any("/*path", gin.WrapH(embedded.Handler))   // gin
//   e.Any("/*", echo.WrapHandler(embedded.Handler)) // echo
```
*Production:* publish `ablyembed` as a normal Go module; `go get` and import.
No platform matrix — the Go toolchain cross-compiles it with the app.

### Node — Express or Fastify middleware

```js
import { AblyServer } from '@ably/embedded-server';
import { mountAblyProxy } from '@ably/embedded-server/express';   // or /fastify

const server = new AblyServer({ apiKey: 'app.key:secret' });
await server.start();                                       // spawn child + /readyz gate

// Express
const proxy = mountAblyProxy(app, { server });              // root (dedicated port)
const httpServer = app.listen(8541);
httpServer.on('upgrade', proxy.upgrade);                    // wire WS upgrades

// Fastify
await registerAblyProxy(fastify, { server });               // WS handled by the plugin
```
*Production:* the prebuilt binary ships via npm `optionalDependencies` keyed
by `os`/`cpu` (the esbuild model), e.g. `@ably/embedded-server-darwin-arm64`;
the embedded server resolves it from there instead of `./bin`. `npm install`
pulls only the right platform binary; the host dev runs no Go.

### .NET — YARP, one call

```csharp
builder.Services.AddAblyEmbedded(opts => opts.ApiKey = "app.key:secret");
var app = builder.Build();
app.MapAblyEmbedded();   // supervises the child + YARP-proxies WS + REST
app.Run();
```
*Production:* ship the binary in a NuGet package under
`runtimes/<rid>/native/ably-server` (e.g. `osx-arm64`, `linux-x64`,
`win-x64`); the embedded server resolves the RID-matched binary at runtime.

### Python — FastAPI/Starlette (ASGI)

```python
from ably_embedded import AblyServer, make_proxy_app

server = AblyServer(api_key="app.key:secret")        # mode="disk", data_dir=... for durability
proxy = make_proxy_app(server, base_path="")         # catch-all ASGI reverse-proxy

@asynccontextmanager
async def lifespan(app):
    await server.start()                              # spawn child + /readyz gate
    yield
    await server.stop()                               # SIGTERM the child, no orphan

# FastAPI serves your routes (and /demo/); `proxy` is the fall-through ASGI app
# for Ably traffic (WS + REST). See examples/fastapi_app.py for the ~20-line wiring.
# Run: uvicorn examples.fastapi_app:app --port 8581   (ABLY_BASE_PATH=/ably for subpath)
```
A WSGI framework (Flask, classic Django) can reverse-proxy REST but **not
WebSockets in-process** — WSGI has no socket-hijack hook, so the WS upgrade
returns 400 (`examples/flask_app.py` demonstrates REST-200 / WS-400). Use
FastAPI/Starlette/Quart/async-Django for the full embed; see
[python/NOTES.md](python/NOTES.md). Note: ably-python ignores the
`ClientOptions` port on its WS transport — fold `host:port` into
`realtime_host` (e.g. `realtime_host="127.0.0.1:8581"`, `tls=False`).
*Production:* ship the binary in a **PyPI platform wheel** (the ruff
precedent); `pip install` pulls the right wheel for the OS/arch.

## Root today, subpath (`/ably`) later

The PoC mounts Ably at the **root** of a dedicated port, which stock SDKs
support now. You can *also* mount it under a **subpath** alongside your app's
own routes — every track supports it (`ABLY_MOUNT=/ably`, or `--mount /ably`
for Go, or `opts.MountPath`):

```
GET /                 → your app's home page
GET /demo/            → the browser demo
ANY /ably/* , WS /ably/ → embedded Ably (the prefix is stripped before the child)
```

This is proven at the wire level — the harness passes with `--base-path /ably`
through Go, Express, Fastify, YARP and FastAPI (`/ably/readyz` is 200 while a
bare `/readyz` is 404). **Caveat (honest):** stock Ably SDKs build
root-rooted URLs and have **no `basePath` option yet**, so a *browser/SDK*
client can't target `/ably` today — only raw clients and the harness can. The
deferred SDK change is a small additive `basePath` option threaded through the
SDK's URL builders ([EMBEDDING-POC.md §6](../../EMBEDDING-POC.md)); once it
lands, the browser demo would point at `/ably` too.

## Logging and diagnostics

The embedded server logs at `info` by default (startup, storage, listen,
connection lifecycle). Where those logs go depends on the track, and none of
the tracks throw them away:

| Track | Where the server's logs go | Turn up / redirect |
|---|---|---|
| **Go** (in-process) | the `*slog.Logger` you pass as `Options.Logger` (nil discards) | it's your app's own logger |
| **Node** | child stdout/stderr forwarded to `process.stderr`, prefixed `[ably-server]` | pass `onChildLog: (line) => yourLogger(line)` to `new AblyServer({ … })`, or `() => {}` to mute |
| **.NET** | child stdout at `Information`, stderr at `Warning`, category `Ably.Embedded` | standard ASP.NET logging filters |
| **Python** | child stdout/stderr forwarded to the `ably_embedded.server` logger (stdout `INFO`, stderr `WARNING`) | standard `logging` config |

Health checks are `GET /healthz` and `GET /readyz`. There is **no metrics,
stats, or admin API yet** (no `/metrics`, pprof, or live-view) — see
[RESULTS.md](RESULTS.md#what-we-havent-addressed-and-how-to-close-it) for the
plan (a read-only `GET /stats` is the intended first step).

## A note on auth over plain HTTP

For local dev over plain `http://`, API-key (basic) auth is the simplest, but
Ably's spec (RSC18) requires TLS for an API key (it's a long-lived secret).
SDKs differ: **ably-js** allows it (warns), **ably-go** refuses by default but
offers `WithInsecureAllowBasicAuthWithoutTLS()`, and **.NET IO.Ably** refuses
with no opt-out. The portable answer for non-TLS local dev is **token auth**
(allowed over http by all SDKs; the embedded server issues tokens at
`POST /keys/{keyName}/requestToken`), or terminate TLS locally. The browser
demo uses the dev key directly for simplicity (it's a throwaway key); a real
app would use an `authUrl`. See [RESULTS.md](RESULTS.md#a-note-on-auth).
