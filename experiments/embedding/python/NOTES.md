# M7 — Python embedding (FastAPI/Starlette + Flask): measured trade-offs

Measured on macOS arm64 (Darwin 25.5.0), CPython **3.14.5**, Go 1.25.11,
ably-python **3.1.2**, FastAPI 0.138.2 / Starlette 1.3.1, uvicorn 0.49.0,
httpx 0.28.1, websockets 15.0.1, Flask 3.1.3, loopback. The Python host app
embeds the prebuilt `ably-server` binary as a supervised child process and
reverse-proxies all realtime (WebSocket) + REST traffic to it on a dedicated
port (EMBEDDING-POC.md §5, §6). **FastAPI/Starlette** is the primary adapter
(ASGI — can proxy WebSockets in-process); **Flask** is the contrast (WSGI —
cannot, by design).

## Headline result

The Go conformance harness passes through **FastAPI/Starlette** at **root**
(all six scenarios) **and** at a **subpath** mount (`/ably`, raw-protocol
scenarios), and the **unmodified `ably` Python SDK** does a realtime pub/sub
round-trip through the proxy. Both fault-injection cases pass.

| | FastAPI root | FastAPI `/ably` subpath |
|---|---|---|
| harness | **PASS 6/6** | **PASS 2/2** (rawpubsub + resume) |
| connect | 3.81 ms | — (raw only) |
| pubsub mean / p99 | 0.32 / 0.41 ms | — |
| restpubsub mean / p99 | 0.88 / 1.11 ms | — |
| history | 10, ordered | — |
| rawpubsub | — | 3 delivered |
| resume-after-drop | RESUMED, lossless | RESUMED, lossless |
| soak (200 msgs) | 0 lost, 0 dup, ~1093 msg/s | — |
| native ably-python smoke | **PASS** (key auth) | n/a (stock SDK has no basePath) |

Raw reports: `../results/python-fastapi.json`, `../results/python-subpath.json`,
`../results/python-smoke.json`, `../results/python-postkill.json`,
`../results/python-fault-injection.log`, `../results/python-flask-rest.log`.

`resume` passing at root and subpath is the key proof: the ASGI proxy forwards
a fresh WS upgrade with the **query string intact** (the API key + `format`
ride there) and relays every protocol frame verbatim in both directions, so
the server's resume-after-drop path (RESUMED flag + exact 3-message gap
replay, zero loss) works identically through the proxy — and through the
`StripPrefix("/ably")` subpath mount too.

## Two key findings (the Python-specific contribution to M4)

### 1. WSGI cannot reverse-proxy WebSockets in-process; ASGI can (the framework split)

This is the headline Python finding and the reason the brief asked for **two**
frameworks. **WSGI** (PEP 3333 — Flask, Django-sync, anything behind
gunicorn-sync/uWSGI) is a synchronous `request -> response` contract with **no
concept of a connection upgrade**: there is no hook to hijack the
`Upgrade: websocket` handshake and hold the socket open. So a Flask app
**cannot proxy the Ably realtime WebSocket in-process at all.** Measured: a WS
upgrade to the Flask host returns `HTTP/1.1 400 BAD REQUEST` from Werkzeug —
it rejects the upgrade request outright (`results/python-flask-rest.log`).

**ASGI** (FastAPI/Starlette/Quart/Django-async) has a first-class `websocket`
scope, so the proxy *can* bridge client<->upstream frames in-process. That is
the entire reason FastAPI is the primary example and Flask is the contrast.

What Flask **can** still do (proven, `results/python-flask-rest.log`):
reverse-proxy the **REST** surface — `GET /time` → 200, publish → 201,
history → 200 — so a server-side SDK that only uses REST works behind Flask.
The realtime transport does not. The honest options for a Flask shop:

1. **REST-only** embedding (what `examples/flask_app.py` does).
2. Put an **ASGI server / ingress in front for the WS path** — run the
   embedded server's WebSocket behind uvicorn + this package's `AblyProxy`,
   or behind nginx/Traefik (which handle `Upgrade`), and keep Flask for the
   app's own HTTP routes. The WS no longer flows *through* Flask.

There is **no clean "Flask proxies WS in-process" answer** — `WsgiToAsgi`
(asgiref) bridges only HTTP, not WebSockets. This matches EMBEDDING-POC.md §10
("weaker proxy story in Python/Java … often pushed to an ingress").

A secondary WSGI finding surfaced while wiring Flask shutdown: `atexit` does
**not** run under a bare `SIGTERM` (the default disposition kills the process
first), so a sync host must install its own signal handler to stop the child
or it orphans it. The ASGI example gets this for free from the lifespan; the
Flask example wires it explicitly (`examples/flask_app.py`).

### 2. ably-python 3.1.2 ignores the `port` option on the realtime WS transport

The official `ably` Python SDK 3.1.2 **does not apply the `port` client option
to the realtime WebSocket URL.** `WebSocketTransport.connect()` builds
`f'{scheme}://{self.host}?{query}'` (`ably/transport/websockettransport.py`
L84) with **no port**, so `AblyRealtime(..., port=8583)` connects to
`ws://127.0.0.1` (i.e. :80) and times out:

```
connect(): attempting to connect to ws://127.0.0.1?key=app.key%3Asecret&v=5
WebSocketTransport.on_ws_connect_done(): exception = [Errno 61] Connect call failed ('127.0.0.1', 80)
... TimeoutError | state: DISCONNECTED | err: 50003 504 Connection cancelled due to request timeout
```

This reproduces against a **standalone** server, so it is an SDK issue, not a
proxy issue. **Workaround (proven, used by the smoke test): fold the port into
the host** — `realtime_host="127.0.0.1:<port>"` — so the SDK builds
`ws://127.0.0.1:<port>?...`. With that, the **unmodified** SDK connects and
does a realtime round-trip:

```
connect(): attempting to connect to ws://127.0.0.1:8583?key=app.key%3Asecret&v=5
ws_connect(): connection established ... CONNECTING -> CONNECTED
SMOKE PASS: unmodified ably-python realtime-websocket round-trip through the proxy via key (basic, over ws query string) — no token needed
```

**Auth finding:** unlike the .NET SDK (which refuses basic-auth REST over
`http://` and needs token auth), ably-python connected and pub/subbed over
non-TLS with **plain key auth — no token needed**. The key rides the WS query
string, which the server accepts. (`rest_host` is accepted but logs a
deprecation warning in favour of `endpoint`.) Productisation note: a real
package would either default SDK guidance to fold host:port, or — better —
this is the same data point as the deferred `basePath` SDK work: the SDK URL
builders need the host knobs threaded through consistently.

## §7 trade-off measurements

### Developer efficiency (time-to-first-message)
- Steps from `pip install` to a working pub/sub round-trip following the
  README: **4** — (1) `pip install ably-embedded` (PoC: `pip install fastapi
  uvicorn websockets httpx`), (2) build/obtain the binary, (3) ~6 lines of
  glue (the embedded server in the lifespan + mount the proxy), (4) point the
  SDK at the port (folding host:port — see finding #2).
- The binary build (`go build`) is a one-time build input; in a real package
  the binary ships via **PyPI platform wheels** so the host dev never runs Go
  (the `ruff` precedent — see Portability).
- Warm-cache inner loop on this machine: `uvicorn examples.fastapi_app:app`
  is ready (child spawned + `/readyz` green) in well under a second; the
  native smoke round-trip completes in ~**32 ms** wall (`python-smoke.json`).

### Developer experience (DX)
- **Integration glue the host-app dev writes (FastAPI): ~6 Ably-specific
  lines** inside an otherwise stock FastAPI app — construct
  `AblyServer(...)`, `await server.start()` /
  `await server.stop()` in the lifespan, `make_proxy_app(server)`, and
  a 3-line fall-through that sends Ably paths to the proxy and everything else
  to FastAPI. `examples/fastapi_app.py` is **89 non-blank/non-comment lines**
  total including the demo route and the subpath variant.
- The reusable package the dev does **not** write: `supervise.py` **215**
  code lines + `proxy.py` **252** code lines (+ a 25-line `__init__`).
- New concepts: ~2 — a supervised child (shared with Node/.NET), and the ASGI
  app-composition shape (a small `__call__(scope, receive, send)` router that
  dispatches by path). Fits idiomatic ASGI; mounting via `app.mount` would
  swallow the WS/lifespan scopes, so the example uses an explicit
  fall-through wrapper — the one mildly non-obvious step, hidden from the
  dev in a productised middleware.
- **Misconfiguration error quality (actual):** a missing binary fails fast on
  `await server.start()` with
  `FileNotFoundError: ably-server binary not found at "<path>". Build it first
  (go build -o bin/ably-server ./cmd/ably-server) or set ABLY_SERVER_BINARY
  to its path.` — clear and actionable; no hang to the ready-timeout.

### Platform portability
- Prebuilt binary, **16,288,434 bytes (~16 MB)**, Mach-O `arm64`; install+run
  needs **no toolchain** on the user's machine (the binary is self-contained
  Go). One per OS/arch.
- **PyPI platform-wheel precedent (the `ruff` model, noted not implemented):**
  `ruff` ships a Rust binary via per-platform wheels — one wheel per
  `(python-tag is `py3`/abi3, platform tag)` such as
  `ruff-x.y.z-py3-none-macosx_11_0_arm64.whl`,
  `…-manylinux_2_17_x86_64.whl`, `…-win_amd64.whl` — each carrying the right
  native binary in the package data; `pip` resolves the matching platform tag
  automatically. A productised `ably-embedded` would do the same: ship
  `ably_embedded/bin/ably-server` inside the platform wheel and resolve it
  relative to `__file__`. The PoC's `resolve_binary_path()` is already
  wheel-shaped (it probes `<pkg>/../bin` first). A pure-Python `sdist` would
  carry no binary and require `ABLY_SERVER_BINARY` (or a build step) — so the
  wheels are the real distribution.
- Multi-platform packaging multiplies registry footprint; macOS notarisation /
  Windows code-signing are real productisation costs (out of PoC scope; on the
  cost ledger per §10).

### Ease of use
| Ergonomic | This track |
|---|---|
| One-liner to start? | Yes — `await server.start()` in the FastAPI lifespan; host runs with one `uvicorn` command (or `python examples/run_fastapi.py`). |
| Auto free-port for the child? | Yes — `pick_free_port()` binds an OS ephemeral loopback port; the dev never picks the internal port. Same bind-then-release TOCTOU as Node/.NET. |
| Auto-shutdown with the app? | Yes — the ASGI **lifespan** `finally` calls `server.stop()` (SIGTERM then SIGKILL on grace). Verified (C2). Flask needs an explicit signal handler (finding #1). |
| Restart on crash? | Yes — an asyncio **watchdog** task awaits the child and respawns on unexpected exit, same port, re-gated on `/readyz`. Verified (C1). |
| Config surface | `AblyServer(api_key=, binary_path=, mode=, log_level=, shutdown_grace=, max_restarts=, ...)` + env `ABLY_SERVER_API_KEY` / `ABLY_SERVER_BINARY`. |

### Reliability (measured)
**Conformance (through FastAPI/Starlette):** PASS — root 6/6 incl. **resume**
(RESUMED, lossless 3-message gap replay) and **soak** (200 messages, **0 lost,
0 duplicates**, ~1,093 msg/s through the proxy); subpath 2/2 (rawpubsub +
resume). Raw: `results/python-fastapi.json`, `results/python-subpath.json`.

A pooling fix mattered here: the first cut created a fresh `httpx.AsyncClient`
per request (REST mean 3.78 ms, soak 274 msg/s). Switching to **one pooled
keep-alive client** for the proxy's lifetime dropped REST mean to ~0.88 ms and
raised soak to ~1,093 msg/s — in the same ballpark as Express (1,005 msg/s).

**Fault injection (run, not described — `results/python-fault-injection.log`):**
- **C1 — `kill -9` the embedded child:** the asyncio watchdog detected the
  exit, respawned a **new PID on the same port**, re-gated on `/readyz`, and a
  fresh `harness --scenarios connect,pubsub` **PASSED** against the respawned
  child. Observed: `old pid 37891 dead, new pid 37917 alive, /readyz green;
  harness PASS`.
- **C2 — SIGTERM the host app:** host exited **cleanly (code 0)**, the
  lifespan logged `embedded ably-server stopped`, and the child was **gone —
  no orphan** (verified by PID liveness check). The example runner
  (`examples/run_fastapi.py`) drives uvicorn's `Server._serve()` with its own
  asyncio signal handlers so it exits **0** (the stock `uvicorn` CLI shuts
  down just as gracefully but exits **143** because `capture_signals()`
  re-raises the SIGTERM — see the runner's docstring).

### Complexity / operational cost
- **Moving parts:** two processes (host app + child) vs Go's one — same as
  Node/.NET. Failure blast-radius is **isolated**: a server crash kills the
  child, not the host, and the watchdog restarts it.
- **Supervision burden:** owned by the package (spawn, ready-gate, watchdog
  restart, graceful stop). The host dev writes none of it.
- **The async/sync seam is the Python-specific cost.** The `AblyServer` is
  async; in an ASGI host (FastAPI) it composes for free in the lifespan. In a
  **sync** host (Flask) the dev must run a private asyncio loop in a daemon
  thread and `run_coroutine_threadsafe` to drive it (see `flask_app.py`) — and
  even then cannot get realtime WS. This is the real "Python is assessed, not
  primary" cost from §4/§10, now measured rather than asserted.
- **Packaging:** per-os/arch **PyPI platform wheels** (the `ruff` model) —
  more complex than "just a dependency", simpler than a C-extension; carries
  the same notarisation/signing costs as the other tracks.
- **Single embedded node = no shared state** (§10): two host instances each
  embed their own `memory`-mode server and won't see each other's channels;
  scale path is `cluster` mode (shared Postgres), not tested here by design.

## How to reproduce

```sh
# from repo root: build inputs
go build -o experiments/embedding/python/bin/ably-server ./cmd/ably-server
go build -o experiments/embedding/harness/harness ./experiments/embedding/harness

cd experiments/embedding/python
python3 -m venv .venv
./.venv/bin/pip install fastapi "uvicorn[standard]" websockets httpx flask ably

./scripts/run-conformance.sh 8581           # root    -> results/python-fastapi.json  (PASS 6/6)
./scripts/run-conformance.sh 8582 /ably      # subpath -> results/python-subpath.json  (PASS 2/2)
./scripts/run-sdk-smoke.sh   8583            # native ably-python -> results/python-smoke.json (PASS)
./scripts/run-fault-injection.sh 8585        # C1 + C2 -> results/python-fault-injection.log
./scripts/run-flask-rest.sh  8588            # Flask REST works, WS unsupported -> results/python-flask-rest.log
```
