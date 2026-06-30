"""Run the FastAPI example app under uvicorn with a graceful-shutdown handler
that exits **0** on SIGTERM/SIGINT — the same contract the Node example
(express-app.js) provides.

Why this exists: ``uvicorn examples.fastapi_app:app`` from the CLI shuts down
*gracefully* on SIGTERM (the lifespan runs, the embedded child is reaped, no
orphan) but the process then exits with **143** (128 + SIGTERM): uvicorn's
``capture_signals()`` deliberately RE-RAISES the captured signal after the
clean shutdown (server.py), so the parent observes the signal. That is correct
signal etiquette, but the fault-injection check wants a host that owns its
shutdown and returns 0.

This runner drives uvicorn's own ``Server._serve()`` directly and installs its
OWN asyncio signal handlers (so uvicorn's re-raising ``capture_signals`` is not
used). On SIGTERM/SIGINT it sets ``should_exit``; uvicorn then runs the ASGI
lifespan shutdown (stopping the embedded child), ``_serve()`` returns, and we
exit 0.

Usage:
    ABLY_PUBLIC_PORT=8585 python examples/run_fastapi.py
    ABLY_BASE_PATH=/ably ABLY_PUBLIC_PORT=8585 python examples/run_fastapi.py
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from pathlib import Path

import uvicorn

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


async def _serve(server: uvicorn.Server) -> None:
    loop = asyncio.get_running_loop()

    def _request_exit() -> None:
        server.should_exit = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _request_exit)

    # _serve() runs startup -> main_loop (until should_exit) -> shutdown,
    # which executes the FastAPI lifespan's shutdown (supervisor.stop()).
    await server._serve()


def main() -> int:
    port = int(os.environ.get("ABLY_PUBLIC_PORT", "8585"))
    config = uvicorn.Config(
        "examples.fastapi_app:app",
        host="127.0.0.1",
        port=port,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    asyncio.run(_serve(server))
    # Reached only after a clean graceful shutdown.
    print("[run_fastapi] graceful shutdown complete, exiting 0", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
