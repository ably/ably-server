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
| **Python** (FastAPI) | `cd experiments/embedding/python && python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt && ABLY_PUBLIC_PORT=8581 uvicorn examples.fastapi_app:app --port 8581` | `python examples/ably_python_smoke.py` (ABLY_PUBLIC_PORT=8581) |

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
import { startEmbeddedServer } from '@ably/embedded-server';
import { mountAblyProxy } from '@ably/embedded-server/express';   // or /fastify

const supervisor = await startEmbeddedServer({ apiKey: 'app.key:secret' });

// Express
const proxy = mountAblyProxy(app, { supervisor });          // root (dedicated port)
const server = app.listen(8541);
server.on('upgrade', proxy.upgrade);                        // wire WS upgrades

// Fastify
await registerAblyProxy(fastify, { supervisor });           // WS handled by the plugin
```
*Production:* the prebuilt binary ships via npm `optionalDependencies` keyed
by `os`/`cpu` (the esbuild model), e.g. `@ably/embedded-server-darwin-arm64`;
the supervisor resolves it from there instead of `./bin`. `npm install` pulls
only the right platform binary; the host dev runs no Go.

### .NET — YARP, one call

```csharp
builder.Services.AddAblyEmbedded(opts => opts.ApiKey = "app.key:secret");
var app = builder.Build();
app.MapAblyEmbedded();   // supervises the child + YARP-proxies WS + REST
app.Run();
```
*Production:* ship the binary in a NuGet package under
`runtimes/<rid>/native/ably-server` (e.g. `osx-arm64`, `linux-x64`,
`win-x64`); the supervisor resolves the RID-matched binary at runtime.

### Python — FastAPI/Starlette (ASGI)

> Status: the Python track is being finalised on branch `embed-poc-py` and
> merges into this branch shortly; the snippet below is its intended shape.

```python
from ably_embedded import lifespan, mount_ably          # supervisor + ASGI proxy
app = FastAPI(lifespan=lifespan)
mount_ably(app)                                          # WS + REST reverse proxy
# uvicorn examples.fastapi_app:app --port 8581
```
A WSGI framework (Flask, classic Django) can reverse-proxy REST but **not
WebSockets in-process** — WS needs an ASGI server (uvicorn/hypercorn) or an
ingress (nginx/Traefik). Use FastAPI/Starlette/aiohttp/async-Django for the
full embed; see [python/NOTES.md](python/NOTES.md).
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
