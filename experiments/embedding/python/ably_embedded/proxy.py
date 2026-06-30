"""ASGI reverse-proxy to the embedded ably-server.

A catch-all that forwards BOTH HTTP (via httpx) and WebSocket (bridged with
the ``websockets`` client) to the supervised child on its loopback port
(EMBEDDING-POC.md §5/§6: dedicated port, no SDK change).

Two mount shapes:

* **Root catch-all** — ``base_path=""``. Every request path maps 1:1 to the
  child. This is what a stock Ably SDK needs (it builds root-rooted URLs).

* **Subpath mount** — ``base_path="/ably"``. The prefix is stripped before
  forwarding, so ``/ably/channels/x`` -> ``/channels/x`` and the WS upgrade at
  ``/ably/`` -> ``/`` on the child. Stock SDKs cannot target a subpath yet
  (no ``basePath`` option, EMBEDDING-POC.md §6), so this is exercised by the
  harness's raw-protocol scenarios via ``--base-path``.

WebSocket fidelity is the load-bearing detail for the ``resume`` scenario:
the proxy forwards the upgrade's **query string** unchanged (the API key and
``format`` ride there) and relays each text/binary frame verbatim in both
directions, so the server's resume handshake (RESUMED flag + exact gap
replay) is byte-for-byte preserved through the proxy.
"""

from __future__ import annotations

import asyncio
from typing import Optional

import httpx
import websockets
from starlette.types import Receive, Scope, Send

# Hop-by-hop headers must not be forwarded across a proxy (RFC 7230 §6.1).
_HOP_BY_HOP = frozenset(
    h.lower()
    for h in (
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
        # httpx sets these itself based on the body it sends:
        "content-length",
        "host",
    )
)


def _normalise_base_path(base_path: str) -> str:
    p = (base_path or "").rstrip("/")
    if p and not p.startswith("/"):
        p = "/" + p
    return p


class AblyProxy:
    """Pure-ASGI reverse-proxy app for the embedded ably-server.

    Reads the upstream port LIVE from the supervisor on every request, so a
    crash-restart onto the same port (or a different one) is transparent.
    """

    def __init__(self, supervisor, *, base_path: str = "") -> None:
        self._supervisor = supervisor
        self.base_path = _normalise_base_path(base_path)
        # One pooled HTTP client for the proxy's lifetime: keep-alive
        # connections to the loopback child instead of a fresh TCP handshake
        # per request. Created lazily so it binds to the running event loop.
        self._client: Optional[httpx.AsyncClient] = None

    def _http_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=30.0,
                limits=httpx.Limits(
                    max_connections=200, max_keepalive_connections=100
                ),
            )
        return self._client

    @property
    def _upstream_host(self) -> str:
        port = self._supervisor.port
        if port is None:
            raise RuntimeError("supervisor has no port yet (not started?)")
        return f"127.0.0.1:{port}"

    async def aclose(self) -> None:
        """Release the pooled HTTP client. Idempotent."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _strip_prefix(self, path: str) -> Optional[str]:
        """Map an incoming path to the upstream path, or None if out of scope."""
        if not self.base_path:
            return path
        if path == self.base_path:
            return "/"
        if path.startswith(self.base_path + "/"):
            return path[len(self.base_path):]
        return None  # not under our mount

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            await self._proxy_http(scope, receive, send)
        elif scope["type"] == "websocket":
            await self._proxy_ws(scope, receive, send)
        elif scope["type"] == "lifespan":
            await self._passthrough_lifespan(scope, receive, send)
        else:  # pragma: no cover - defensive
            raise RuntimeError(f"unsupported ASGI scope: {scope['type']!r}")

    async def _passthrough_lifespan(self, scope, receive, send) -> None:
        # The proxy itself owns no startup/shutdown; the host app's lifespan
        # drives the supervisor. Just complete the protocol so a standalone
        # mount doesn't hang.
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return

    # ------------------------------------------------------------------ HTTP

    async def _proxy_http(self, scope: Scope, receive: Receive, send: Send) -> None:
        upstream_path = self._strip_prefix(scope["path"])
        if upstream_path is None:
            await self._send_plain(send, 404, "not found")
            return

        qs = scope.get("query_string", b"").decode("latin-1")
        url = f"http://{self._upstream_host}{upstream_path}"
        if qs:
            url += "?" + qs

        headers = [
            (k.decode("latin-1"), v.decode("latin-1"))
            for k, v in scope.get("headers", [])
            if k.decode("latin-1").lower() not in _HOP_BY_HOP
        ]
        body = await self._read_body(receive)

        try:
            upstream = await self._http_client().request(
                scope["method"],
                url,
                headers=headers,
                content=body,
            )
        except Exception as exc:
            await self._send_plain(
                send, 502, f"embedded ably-server proxy error: {exc}"
            )
            return

        out_headers = [
            (k.encode("latin-1"), v.encode("latin-1"))
            for k, v in upstream.headers.items()
            if k.lower() not in _HOP_BY_HOP
        ]
        await send(
            {
                "type": "http.response.start",
                "status": upstream.status_code,
                "headers": out_headers,
            }
        )
        await send(
            {"type": "http.response.body", "body": upstream.content, "more_body": False}
        )

    @staticmethod
    async def _read_body(receive: Receive) -> bytes:
        chunks = []
        while True:
            message = await receive()
            if message["type"] != "http.request":
                break
            chunks.append(message.get("body", b""))
            if not message.get("more_body", False):
                break
        return b"".join(chunks)

    @staticmethod
    async def _send_plain(send: Send, status: int, text: str) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [(b"content-type", b"text/plain; charset=utf-8")],
            }
        )
        await send(
            {"type": "http.response.body", "body": text.encode("utf-8")}
        )

    # ------------------------------------------------------------- WebSocket

    async def _proxy_ws(self, scope: Scope, receive: Receive, send: Send) -> None:
        upstream_path = self._strip_prefix(scope["path"])
        if upstream_path is None:
            await receive()  # consume websocket.connect
            await send({"type": "websocket.close", "code": 1008})
            return

        # Wait for the client's connect, then open the upstream WS. We must
        # accept downstream only AFTER upstream is open so we can mirror the
        # negotiated subprotocol and never accept a connection the child
        # would have rejected.
        connect = await receive()
        if connect["type"] != "websocket.connect":
            return

        qs = scope.get("query_string", b"").decode("latin-1")
        scheme = "ws"
        upstream_url = f"{scheme}://{self._upstream_host}{upstream_path}"
        if qs:
            upstream_url += "?" + qs

        # Forward the client's requested subprotocols and a faithful header
        # set (drop hop-by-hop + the headers the websockets client manages).
        subprotocols = scope.get("subprotocols") or None
        fwd_headers = {
            k.decode("latin-1"): v.decode("latin-1")
            for k, v in scope.get("headers", [])
            if k.decode("latin-1").lower()
            not in _HOP_BY_HOP
            | {
                "sec-websocket-key",
                "sec-websocket-version",
                "sec-websocket-extensions",
                "sec-websocket-protocol",
            }
        }

        try:
            upstream_ws = await websockets.connect(
                upstream_url,
                subprotocols=subprotocols,
                additional_headers=fwd_headers,
                # No client-side frame size cap: faithfully relay whatever the
                # server emits (resume replays can be large).
                max_size=None,
                open_timeout=10,
            )
        except Exception:
            await send({"type": "websocket.close", "code": 1011})
            return

        negotiated = getattr(upstream_ws, "subprotocol", None)
        accept_msg = {"type": "websocket.accept"}
        if negotiated:
            accept_msg["subprotocol"] = negotiated
        await send(accept_msg)

        # Teardown is centralised in the outer `finally` below, not split
        # across the two pumps. The pumps do NOT close anything in their own
        # `finally` (awaiting a close while being cancelled is the brittle
        # case the websockets docs warn about); they only relay frames. When
        # either pump returns, the outer code cancels the other, closes the
        # upstream once, and sends the downstream close exactly once.
        client_disconnected = False

        async def client_to_upstream() -> None:
            nonlocal client_disconnected
            while True:
                message = await receive()
                mtype = message["type"]
                if mtype == "websocket.receive":
                    if message.get("text") is not None:
                        await upstream_ws.send(message["text"])
                    elif message.get("bytes") is not None:
                        await upstream_ws.send(message["bytes"])
                elif mtype == "websocket.disconnect":
                    client_disconnected = True
                    return

        async def upstream_to_client() -> None:
            try:
                async for frame in upstream_ws:
                    if isinstance(frame, (bytes, bytearray)):
                        await send(
                            {"type": "websocket.send", "bytes": bytes(frame)}
                        )
                    else:
                        await send({"type": "websocket.send", "text": frame})
            except websockets.ConnectionClosed:
                pass

        c2u = asyncio.create_task(client_to_upstream())
        u2c = asyncio.create_task(upstream_to_client())
        try:
            _, pending = await asyncio.wait(
                {c2u, u2c}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        finally:
            # Close the upstream exactly once (idempotent in websockets).
            try:
                await upstream_ws.close()
            except Exception:
                pass
            # Close the downstream exactly once, and only if the client has
            # not already disconnected (sending on a gone socket would raise).
            if not client_disconnected:
                try:
                    await send({"type": "websocket.close", "code": 1000})
                except Exception:
                    pass


def make_proxy_app(supervisor, *, base_path: str = "") -> AblyProxy:
    """Build an ASGI app that reverse-proxies to the embedded server."""
    return AblyProxy(supervisor, base_path=base_path)
