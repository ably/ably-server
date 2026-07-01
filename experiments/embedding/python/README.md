# `ably-embedded` (Python) — embed ably-server in a Python web app (PoC)

Embed the `ably-server` realtime endpoint **inside your own Python web app**:
the package supervises the prebuilt `ably-server` binary as a child process on
a free loopback port, and a catch-all **ASGI reverse-proxy** forwards HTTP +
WebSocket traffic to it on a dedicated port. An **unmodified** Ably SDK points
at your app's host+port and gets pub/sub, history, and resume — no separate
realtime service to run (EMBEDDING-POC.md §5/§6).

> **Framework support is split by the WSGI/ASGI line.**
> **FastAPI / Starlette / Quart / any ASGI host** can proxy the realtime
> WebSocket in-process — this is the primary, full-feature path.
> **Flask / Django-sync / any WSGI host** can proxy **REST only**; WSGI has no
> WebSocket-upgrade hook, so realtime needs an ASGI server or an ingress in
> front. See `examples/flask_app.py` and `NOTES.md` for the full finding.

## Layout

```
python/
  ably_embedded/
    supervise.py   # spawn + supervise the binary (free port, /readyz, restart, stop)
    proxy.py       # catch-all ASGI reverse-proxy (HTTP via httpx, WS via websockets)
    __init__.py    # public API
  examples/
    fastapi_app.py        # primary: FastAPI host, root catch-all OR /ably subpath, serves /demo/
    run_fastapi.py        # runner that exits 0 on SIGTERM (graceful)
    flask_app.py          # contrast: Flask (WSGI) — REST proxy works, WS does not
    ably_python_smoke.py  # native `ably` SDK round-trip through the proxy
  scripts/                # reproducible run scripts (conformance, smoke, fault injection, flask)
  bin/ably-server         # the prebuilt binary (build input, gitignored)
  NOTES.md                # measured §7 trade-offs + the two key findings
```

## Run it (dev / local)

```sh
# 1. Build the server binary (one-time build input; a real package ships it as
#    a PyPI platform wheel — see "Production" below). From the repo root:
go build -o experiments/embedding/python/bin/ably-server ./cmd/ably-server

# 2. Create a venv and install deps.
cd experiments/embedding/python
python3 -m venv .venv
./.venv/bin/pip install fastapi "uvicorn[standard]" websockets httpx flask ably

# 3. Run the FastAPI host (root catch-all — what a stock SDK needs).
ABLY_PUBLIC_PORT=8581 ./.venv/bin/uvicorn examples.fastapi_app:app \
  --host 127.0.0.1 --port 8581
#    or, for a host that exits 0 on SIGTERM:
ABLY_PUBLIC_PORT=8581 ./.venv/bin/python examples/run_fastapi.py
```

Now the app's own origin **is** an Ably endpoint:

```sh
curl http://127.0.0.1:8581/readyz          # 200 — proxied to the embedded child
open  http://127.0.0.1:8581/demo/           # the ably-js browser demo (two tabs = live chat)
```

### Point an SDK at it

ably-js (browser/Node) — the demo at `/demo/` already does this:

```js
new Ably.Realtime({ key: 'app.key:secret',
  restHost: '127.0.0.1', realtimeHost: '127.0.0.1', port: 8581, tls: false });
```

ably-python — **fold the port into the host** (ably-python 3.1.2 ignores the
`port` option on the realtime WS transport; see `NOTES.md` finding #2):

```python
from ably import AblyRealtime
client = AblyRealtime(key="app.key:secret",
    realtime_host="127.0.0.1:8581", rest_host="127.0.0.1:8581",
    tls=False, use_binary_protocol=False)
```

Key auth over non-TLS works for ably-python — no token needed.

### Subpath mount (Ably under `/ably`, alongside your own routes)

```sh
ABLY_BASE_PATH=/ably ABLY_PUBLIC_PORT=8582 \
  ./.venv/bin/uvicorn examples.fastapi_app:app --host 127.0.0.1 --port 8582
curl http://127.0.0.1:8582/app/hello        # the app's own route
curl http://127.0.0.1:8582/ably/readyz       # Ably, mounted under /ably
```

Stock SDKs can't target a subpath yet (no `basePath` option — EMBEDDING-POC.md
§6), but the raw protocol can; the conformance harness exercises it with
`--base-path /ably`.

### Flask (REST only)

```sh
ABLY_PUBLIC_PORT=8588 ./.venv/bin/python examples/flask_app.py
curl -u app.key:secret http://127.0.0.1:8588/time          # 200 (REST proxied)
# A WebSocket upgrade returns an error: WSGI cannot proxy WS in-process.
```

## Verify (the numbers in NOTES.md)

```sh
# from repo root, build the harness too:
go build -o experiments/embedding/harness/harness ./experiments/embedding/harness

cd experiments/embedding/python
./scripts/run-conformance.sh 8581          # root harness    -> PASS 6/6
./scripts/run-conformance.sh 8582 /ably     # subpath harness -> PASS 2/2
./scripts/run-sdk-smoke.sh   8583           # native ably-python round-trip -> PASS
./scripts/run-fault-injection.sh 8585       # C1 kill -9 respawn + C2 clean shutdown
./scripts/run-flask-rest.sh  8588           # Flask REST works, WS unsupported
```

## Configuration

| Setting | Where | Default |
|---|---|---|
| API key | `AblyServer(api_key=)` / env `ABLY_SERVER_API_KEY` | `app.key:secret` |
| Binary path | `AblyServer(binary_path=)` / env `ABLY_SERVER_BINARY` | `<pkg>/../bin/ably-server` |
| Server mode | `AblyServer(mode=)` | `memory` |
| Internal port | `AblyServer(port=)` | OS-assigned free port |
| Shutdown grace | `AblyServer(shutdown_grace=)` | `10s` |
| Max crash-restarts | `AblyServer(max_restarts=)` | `5` |
| Public port (example) | env `ABLY_PUBLIC_PORT` | 8581/8585/8588 |
| Subpath (example) | env `ABLY_BASE_PATH` | `""` (root) |

## Production package shape (not built in the PoC)

The distribution is **PyPI platform wheels**, the same model `ruff` uses to
ship a Rust binary: one wheel per `(py3/abi3, platform tag)` —
`ably_embedded-x.y.z-py3-none-macosx_11_0_arm64.whl`,
`…-manylinux_2_17_x86_64.whl`, `…-win_amd64.whl` — each carrying the matching
`ably-server` binary inside the package data (`ably_embedded/bin/`). `pip`
resolves the right platform tag automatically, so the host dev never installs
Go. `resolve_binary_path()` already probes `<pkg>/../bin` first, so this is a
packaging change, not a code change. (An `sdist` carries no binary; set
`ABLY_SERVER_BINARY` or build it.) macOS notarisation / Windows code-signing
are real productisation costs, out of PoC scope — see EMBEDDING-POC.md §10.
