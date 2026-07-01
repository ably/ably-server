"""Example Flask host app embedding ably-server — and an HONEST demonstration
of the WSGI WebSocket limitation (EMBEDDING-POC.md §10: "weaker proxy story
in Python/Java").

THE FLASK / WSGI FINDING (the useful contrast vs FastAPI/ASGI):

  Flask is a **WSGI** framework. WSGI is a synchronous request->response
  contract (PEP 3333) with **no concept of a connection upgrade**: the
  protocol cannot express "hand me the raw socket so I can speak WebSocket".
  So a Flask app **cannot reverse-proxy the Ably realtime WebSocket
  in-process** — there is no hook in WSGI to hijack the `Upgrade: websocket`
  handshake. (Async frameworks built on ASGI — FastAPI/Starlette — CAN,
  because ASGI has a first-class `websocket` scope; that is the whole reason
  the primary example is FastAPI.)

  Consequences / the honest options for a Flask shop:
    1. REST-only embedding (what THIS file does): Flask happily reverse-
       proxies the Ably REST surface (`/channels`, `/keys`, `/time`,
       `/healthz`, `/readyz`) to the embedded child. An Ably SDK using REST
       (e.g. server-side publish) works. The realtime WebSocket does NOT.
    2. Put an ASGI server / ingress in front for the WS upgrade: run the
       embedded server's WS behind an ASGI process (uvicorn + the AblyProxy
       in this package) or an ingress (nginx/Traefik) that handles `Upgrade`,
       and keep Flask for the app's own HTTP routes. The WS no longer goes
       *through* Flask.
    3. Run Flask under an ASGI bridge that supports WebSockets (e.g. behind
       Hypercorn via WsgiToAsgi only handles HTTP, not WS — so you still need
       a separate ASGI WS path). There is no clean "Flask proxies WS
       in-process" answer; that is the finding.

  This file proves option 1 (REST proxy works) and refuses WS loudly so the
  limitation is visible, not hidden.

Run:
    ABLY_PUBLIC_PORT=8588 python examples/flask_app.py
Then REST works, e.g.:
    curl -u app.key:secret http://127.0.0.1:8588/time
    curl -u app.key:secret -H 'content-type: application/json' \
         -d '{"name":"ev","data":"hi"}' \
         http://127.0.0.1:8588/channels/demo/messages
A WebSocket upgrade to ws://127.0.0.1:8588/ returns HTTP 501 with an
explanatory message (the limitation, surfaced honestly).
"""

from __future__ import annotations

import asyncio
import atexit
import os
import signal
import sys
import threading
from pathlib import Path

import httpx
from flask import Flask, Response, request

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ably_embedded import AblyServer  # noqa: E402

PORT = int(os.environ.get("ABLY_PUBLIC_PORT", "8588"))

# --- Embedded-server lifecycle on a dedicated background asyncio loop -------
# The AblyServer is async; Flask/WSGI is sync. Run a private event loop in a
# daemon thread and drive the server on it. This is the WSGI-world cost of
# embedding an async-supervised child: you manage the loop yourself.
_loop = asyncio.new_event_loop()
_loop_thread = threading.Thread(target=_loop.run_forever, daemon=True)
_loop_thread.start()


def _run(coro):
    return asyncio.run_coroutine_threadsafe(coro, _loop).result()


server = AblyServer(
    api_key=os.environ.get("ABLY_SERVER_API_KEY", "app.key:secret"),
    on_event=lambda event, payload: print(
        f"[flask-app] embedded server: {event} {payload}", flush=True
    ),
)
_run(server.start())
print(
    f"[flask-app] embedded ably-server ready on internal port {server.port} "
    f"(pid {server.pid})",
    flush=True,
)


_stopped = threading.Event()


def _shutdown() -> None:
    if _stopped.is_set():
        return
    _stopped.set()
    try:
        _run(server.stop())
        print("[flask-app] embedded ably-server stopped", flush=True)
    except Exception:
        pass


# atexit covers a normal interpreter exit (Ctrl-C in the dev server), but
# NOT a bare SIGTERM — under SIGTERM the default disposition terminates the
# process before atexit runs, orphaning the child. So we install explicit
# signal handlers too. (This is itself a WSGI-world finding: a sync host has
# to wire its own lifecycle; the ASGI example gets it from the lifespan.)
atexit.register(_shutdown)


def _on_signal(_signum, _frame):
    # Stop the child, then exit 0 directly. We deliberately do NOT re-raise
    # the signal: the Flask dev server and this script can share a process
    # group, and re-raising would leak the signal to siblings. os._exit(0)
    # after a clean child teardown is the unambiguous "graceful" outcome.
    _shutdown()
    os._exit(0)


for _sig in (signal.SIGTERM, signal.SIGINT):
    signal.signal(_sig, _on_signal)


app = Flask(__name__)

# A synchronous, pooled HTTP client to the embedded child (loopback).
_client = httpx.Client(
    timeout=30.0,
    limits=httpx.Limits(max_connections=100, max_keepalive_connections=50),
)

_HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "content-length", "host",
}


@app.route(
    "/<path:subpath>",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
)
@app.route(
    "/",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
    defaults={"subpath": ""},
)
def proxy(subpath: str):
    # Refuse WebSocket upgrades LOUDLY — this is the WSGI limitation, made
    # visible instead of silently mis-handled.
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return Response(
            "WSGI (Flask) cannot reverse-proxy a WebSocket upgrade in-process. "
            "Use the FastAPI/ASGI example for realtime, or place an ASGI "
            "server / ingress in front for the WS path. See flask_app.py.",
            status=501,
            mimetype="text/plain",
        )

    url = f"http://127.0.0.1:{server.port}/{subpath}"
    fwd_headers = {
        k: v for k, v in request.headers.items() if k.lower() not in _HOP_BY_HOP
    }
    upstream = _client.request(
        request.method,
        url,
        params=request.args,
        headers=fwd_headers,
        content=request.get_data(),
    )
    resp_headers = [
        (k, v) for k, v in upstream.headers.items() if k.lower() not in _HOP_BY_HOP
    ]
    return Response(
        upstream.content, status=upstream.status_code, headers=resp_headers
    )


if __name__ == "__main__":
    # threaded=True so concurrent REST requests don't serialise behind one
    # another (each gets a worker thread; the httpx client is thread-safe).
    app.run(host="127.0.0.1", port=PORT, threaded=True)
