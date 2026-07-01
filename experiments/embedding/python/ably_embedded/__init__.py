"""ably_embedded — embed ably-server in a Python web app (PoC).

Public surface:

    from ably_embedded import AblyServer, start_embedded_server
    from ably_embedded import make_proxy_app

AblyServer spawns + supervises the prebuilt ably-server binary on a free
loopback port; make_proxy_app returns a catch-all ASGI app that
reverse-proxies HTTP + WebSocket traffic to it (EMBEDDING-POC.md §5/§6).
"""

from .proxy import AblyProxy, make_proxy_app
from .supervise import (
    AblyServer,
    AblyServerSupervisor,  # back-compat alias for AblyServer
    pick_free_port,
    resolve_binary_path,
    start_embedded_server,
    wait_for_ready,
)

__all__ = [
    "AblyServer",
    "AblyServerSupervisor",
    "start_embedded_server",
    "resolve_binary_path",
    "pick_free_port",
    "wait_for_ready",
    "AblyProxy",
    "make_proxy_app",
]
