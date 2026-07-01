"""Example FastAPI/Starlette host app embedding ably-server.

This is the glue a host-app developer writes. The host:

  * starts the supervisor (spawns + supervises the ably-server child),
  * mounts the ASGI reverse-proxy as a catch-all on a dedicated port,
  * serves the browser demo at /demo/,
  * shuts the child down cleanly when the app stops (no orphan).

Run (root catch-all — what a stock Ably SDK needs):

    ABLY_PUBLIC_PORT=8581 \
      uvicorn examples.fastapi_app:app --host 127.0.0.1 --port 8581

Subpath variant (Ably mounted under /ably alongside the app's own routes;
exercised by `harness --base-path /ably`):

    ABLY_BASE_PATH=/ably ABLY_PUBLIC_PORT=8582 \
      uvicorn examples.fastapi_app:app --host 127.0.0.1 --port 8582

Then point an unmodified Ably SDK at 127.0.0.1:<ABLY_PUBLIC_PORT> (tls=False),
or open http://127.0.0.1:<ABLY_PUBLIC_PORT>/demo/ in two browser tabs.
"""

from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse

# Allow `uvicorn examples.fastapi_app:app` from the python/ dir.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ably_embedded import AblyServer, make_proxy_app  # noqa: E402

# Where the embedded server is mounted. "" = root catch-all (stock SDK).
# "/ably" = subpath alongside the app's own routes.
BASE_PATH = os.environ.get("ABLY_BASE_PATH", "").rstrip("/")
DEMO_HTML = (
    Path(__file__).resolve().parent.parent.parent / "demo" / "index.html"
)

# A single embedded server for the app's lifetime.
server = AblyServer(
    api_key=os.environ.get("ABLY_SERVER_API_KEY", "app.key:secret"),
    on_event=lambda event, payload: print(
        f"[fastapi-app] embedded server: {event} {payload}", flush=True
    ),
)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    port = await server.start()
    print(
        f"[fastapi-app] embedded ably-server ready on internal port {port} "
        f"(pid {server.pid}); base_path={BASE_PATH or '/'}",
        flush=True,
    )
    try:
        yield
    finally:
        # Stop the child cleanly on app shutdown so there is no orphan, then
        # release the proxy's pooled HTTP connections.
        await server.stop()
        await _proxy.aclose()
        print("[fastapi-app] embedded ably-server stopped", flush=True)


app = FastAPI(lifespan=lifespan)


@app.get("/demo/")
@app.get("/demo")
async def demo() -> FileResponse:
    """Serve the unmodified ably-js browser demo from the host's own origin."""
    return FileResponse(DEMO_HTML, media_type="text/html")


# The ASGI reverse-proxy to the embedded server. Mounting it via `app.mount`
# would consume the path prefix and hide the WS/lifespan scopes from our
# proxy, so we install it as a fall-through ASGI app instead (below).
_proxy = make_proxy_app(server, base_path=BASE_PATH)


if BASE_PATH:
    # SUBPATH MODE: the app keeps its own routes; only /ably/* is proxied.
    # An app-native route proves the host app coexists with embedded Ably.
    @app.get("/app/hello")
    async def app_hello() -> JSONResponse:
        return JSONResponse(
            {"app": "fastapi", "note": f"Ably is mounted at {BASE_PATH}"}
        )

    # Wrap the FastAPI app: requests under BASE_PATH go to the proxy, the
    # rest to FastAPI. WebSocket + HTTP both honoured.
    _fastapi_app = app

    class _Router:
        def __init__(self, base_path: str):
            self.base_path = base_path

        async def __call__(self, scope, receive, send):
            if scope["type"] == "lifespan":
                # The FastAPI app owns the lifespan (it starts the server).
                await _fastapi_app(scope, receive, send)
                return
            path = scope.get("path", "")
            if path == self.base_path or path.startswith(self.base_path + "/"):
                await _proxy(scope, receive, send)
            else:
                await _fastapi_app(scope, receive, send)

    app = _Router(BASE_PATH)  # type: ignore[assignment]
else:
    # ROOT MODE: everything except /demo is the embedded Ably endpoint.
    # FastAPI handles /demo; the proxy is the catch-all for the rest
    # (the WS upgrade at "/", REST under /channels, /keys, /time, /healthz...).
    _fastapi_app = app

    class _RootRouter:
        async def __call__(self, scope, receive, send):
            if scope["type"] == "lifespan":
                await _fastapi_app(scope, receive, send)
                return
            path = scope.get("path", "")
            if path == "/demo" or path.startswith("/demo/"):
                await _fastapi_app(scope, receive, send)
            else:
                await _proxy(scope, receive, send)

    app = _RootRouter()  # type: ignore[assignment]
