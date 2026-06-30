"""Supervisor for an embedded ably-server child process.

Responsibilities (EMBEDDING-POC.md §5):

  1. Resolve the prebuilt per-platform binary path.
  2. Pick a FREE TCP port and pass --listen 127.0.0.1:<port>.
  3. Spawn the child with the API key in the environment.
  4. Poll /readyz until green (with a timeout).
  5. Restart the child on UNEXPECTED exit (with backoff).
  6. stop(): SIGTERM the child, then SIGKILL on grace timeout. No orphan.

asyncio-native: the watchdog and the readiness poll are coroutines, so the
supervisor composes cleanly with an ASGI app's lifespan. No dependency
beyond the standard library + httpx (already a proxy dependency).
"""

from __future__ import annotations

import asyncio
import os
import platform
import signal
import socket
import time
from pathlib import Path
from typing import Awaitable, Callable, Optional

import httpx

_HERE = Path(__file__).resolve().parent


def resolve_binary_path() -> str:
    """Resolve the path to the prebuilt ably-server binary.

    Real-package behaviour (NOT implemented in the PoC): a productised PyPI
    wheel ships the per-platform binary inside the package data and resolves
    it relative to ``__file__`` — the ``ruff`` model (one platform wheel per
    ``os``/``arch`` tag, the binary in ``<pkg>/bin/``). The resolver below is
    already wheel-shaped: it probes ``<pkg>/../bin`` first, which is exactly
    where a wheel would place it.

    PoC behaviour: honour ``ABLY_SERVER_BINARY``; else fall back to
    ``<python>/bin/ably-server`` (``.exe`` on Windows).
    """
    env = os.environ.get("ABLY_SERVER_BINARY")
    if env:
        return env
    exe = "ably-server.exe" if platform.system() == "Windows" else "ably-server"
    # ably_embedded/ -> python/ -> bin/
    return str((_HERE.parent / "bin" / exe).resolve())


def pick_free_port() -> int:
    """Ask the OS for a free TCP port on 127.0.0.1.

    Bind to :0, read back the assigned port, release it. There is an inherent
    (tiny) race between release and the child re-binding; in practice the
    child grabs it immediately. This is the standard "give me a free port"
    idiom and matches the Node/.NET tracks' approach (so the free-port TOCTOU
    note in their NOTES applies identically here).
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def wait_for_ready(
    port: int,
    *,
    timeout_s: float = 10.0,
    interval_s: float = 0.1,
) -> None:
    """Poll http://127.0.0.1:<port>/readyz until 200 or timeout."""
    deadline = time.monotonic() + timeout_s
    last_err: Optional[str] = None
    async with httpx.AsyncClient(timeout=min(interval_s * 5, 2.0)) as client:
        while time.monotonic() < deadline:
            try:
                resp = await client.get(f"http://127.0.0.1:{port}/readyz")
                if resp.status_code == 200:
                    return
                last_err = f"/readyz returned {resp.status_code}"
            except Exception as exc:  # connection refused while booting, etc.
                last_err = str(exc)
            await asyncio.sleep(interval_s)
    raise RuntimeError(
        f"ably-server did not become ready on 127.0.0.1:{port} within "
        f"{int(timeout_s * 1000)}ms"
        + (f" (last: {last_err})" if last_err else "")
    )


class AblyServerSupervisor:
    """Supervises a single embedded ably-server child process.

    Lifecycle is async so it slots into an ASGI lifespan:

        sup = AblyServerSupervisor(api_key=...)
        await sup.start()        # spawn + wait for /readyz
        ...                      # proxy to sup.port
        await sup.stop()         # SIGTERM, then SIGKILL on grace timeout

    On unexpected child exit the watchdog respawns it (capped, with backoff)
    on the SAME port — so a proxy that captured ``supervisor.port`` once stays
    valid across a crash-restart.
    """

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        binary_path: Optional[str] = None,
        port: Optional[int] = None,
        mode: str = "memory",
        log_level: str = "error",
        shutdown_grace: str = "10s",
        ready_timeout_s: float = 10.0,
        max_restarts: int = 5,
        on_event: Optional[Callable[[str, object], None]] = None,
    ) -> None:
        self.api_key = api_key or os.environ.get(
            "ABLY_SERVER_API_KEY", "app.key:secret"
        )
        self.binary_path = binary_path or resolve_binary_path()
        self._fixed_port = port
        self.mode = mode
        self.log_level = log_level
        self.shutdown_grace = shutdown_grace
        self.ready_timeout_s = ready_timeout_s
        self.max_restarts = max_restarts
        self._on_event = on_event or (lambda _e, _p: None)

        self.port: Optional[int] = None
        self.proc: Optional[asyncio.subprocess.Process] = None
        self._stopping = False
        self._started = False
        self._failed = False  # terminal: crashed past max_restarts
        self._restart_count = 0
        self._watchdog: Optional[asyncio.Task] = None

    @property
    def pid(self) -> Optional[int]:
        return self.proc.pid if self.proc else None

    @property
    def failed(self) -> bool:
        """True once the child has crashed past max_restarts and the
        supervisor has given up — it will not come back on its own."""
        return self._failed

    def _emit(self, event: str, payload: object = None) -> None:
        try:
            self._on_event(event, payload)
        except Exception:
            pass  # an instrumentation callback must never break supervision

    async def start(self) -> int:
        """Spawn the child and wait until /readyz is green. Returns the port."""
        if self._started:
            raise RuntimeError("supervisor already started")
        self._started = True

        if not Path(self.binary_path).exists():
            # Fail fast and actionably — the equivalent of the Node/.NET
            # "binary not found" guard, surfaced as a normal raised error so
            # an awaited start() rejects rather than the watchdog firing.
            raise FileNotFoundError(
                f'ably-server binary not found at "{self.binary_path}". '
                f"Build it first (go build -o bin/ably-server ./cmd/ably-server) "
                f"or set ABLY_SERVER_BINARY to its path."
            )

        self.port = self._fixed_port or pick_free_port()
        await self._spawn_child()
        await wait_for_ready(self.port, timeout_s=self.ready_timeout_s)
        self._emit("ready", self.port)
        return self.port

    async def _spawn_child(self) -> None:
        # Never spawn while stopping — this guard closes the respawn-vs-stop
        # race: the watchdog can call _spawn_child after stop() set _stopping,
        # which would otherwise leave a fresh orphan after stop() returns.
        if self._stopping:
            return
        args = [
            "--mode", self.mode,
            "--listen", f"127.0.0.1:{self.port}",
            "--log-level", self.log_level,
            "--shutdown-grace", self.shutdown_grace,
        ]
        env = {**os.environ, "ABLY_SERVER_API_KEY": self.api_key}
        self.proc = await asyncio.create_subprocess_exec(
            self.binary_path,
            *args,
            env=env,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        # If stop() raced in between the _stopping check and now, undo: kill the
        # child we just spawned so it cannot outlive stop().
        if self._stopping:
            try:
                self.proc.kill()
            except ProcessLookupError:
                pass
            return
        self._emit("spawn", self.proc.pid)
        self._watchdog = asyncio.create_task(self._watch(self.proc))

    async def _watch(self, proc: asyncio.subprocess.Process) -> None:
        """Await the child's exit; respawn it if it died unexpectedly."""
        code = await proc.wait()
        self._emit("exit", code)
        if self._stopping:
            return
        # Unexpected exit: restart on a capped exponential backoff.
        self._restart_count += 1
        if self._restart_count > self.max_restarts:
            self._failed = True  # terminal: never coming back on its own
            self._emit(
                "error",
                f"ably-server crashed {self._restart_count} times; giving up",
            )
            return
        backoff_s = min(0.1 * 2 ** (self._restart_count - 1), 2.0)
        self._emit("restart", self._restart_count)
        await asyncio.sleep(backoff_s)
        if self._stopping:
            return
        try:
            await self._spawn_child()
            if self._stopping:
                return  # stop() raced in during the spawn; let it own teardown
            await wait_for_ready(self.port, timeout_s=self.ready_timeout_s)
            # A clean re-ready resets the counter so isolated, far-apart
            # crashes don't accumulate toward the give-up threshold.
            self._restart_count = 0
            self._emit("ready", self.port)
        except Exception as exc:  # respawn failed
            self._emit("error", str(exc))

    async def stop(self, *, timeout_s: float = 12.0) -> None:
        """Gracefully stop the child: SIGTERM, then SIGKILL on timeout.

        Idempotent. Sets _stopping first (so the watchdog will not respawn and
        _spawn_child is a no-op), then drains the current watchdog and child.
        Loops because a respawn may have been in flight when _stopping was set:
        we keep cancelling whatever watchdog and terminating whatever child is
        current until both are quiescent, so no respawned child is orphaned.
        """
        self._stopping = True
        # A respawn in flight may keep reassigning self._watchdog / self.proc.
        # Iterate until we observe a stable, quiescent state twice.
        for _ in range(10):
            wd = self._watchdog
            if wd is not None and not wd.done():
                wd.cancel()
                try:
                    await wd
                except (asyncio.CancelledError, Exception):
                    pass
            proc = self.proc
            if proc is not None and proc.returncode is None:
                try:
                    proc.send_signal(signal.SIGTERM)
                    try:
                        await asyncio.wait_for(proc.wait(), timeout=timeout_s)
                    except asyncio.TimeoutError:
                        proc.kill()
                        await proc.wait()
                except ProcessLookupError:
                    pass
            # Quiescent when the watchdog is done and no live child remains and
            # nothing started a new one since we began this iteration.
            wd_done = self._watchdog is None or self._watchdog.done()
            no_live = self.proc is None or self.proc.returncode is not None
            if wd_done and no_live and self._watchdog is wd and self.proc is proc:
                break
        self.proc = None


async def start_embedded_server(**kwargs) -> AblyServerSupervisor:
    """Convenience: construct + start a supervisor in one call."""
    sup = AblyServerSupervisor(**kwargs)
    await sup.start()
    return sup
