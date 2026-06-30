"""Native ably-python smoke test: prove the OFFICIAL `ably` SDK works through
the host-app proxy.

Points the unmodified SDK at the public port with tls=False and asserts a
realtime publish->subscribe round-trip. Records the outcome (and the auth /
endpoint finding) to results/python-smoke.json.

THE ably-python ENDPOINT FINDING (real SDK limitation, measured here):
ably-python 3.1.2 **ignores the `port` client option for the realtime
WebSocket transport** — `WebSocketTransport.connect()` builds
`f'{scheme}://{self.host}?{query}'` (websockettransport.py L84) with NO port,
so `port=8583` connects to ws://127.0.0.1 (i.e. :80) and times out. This is
independent of the proxy (it reproduces against a standalone server). The
working path, used below, is to FOLD the port into the host:

    realtime_host="127.0.0.1:<port>"   # -> ws://127.0.0.1:<port>?...

With that, the UNMODIFIED SDK connects and does a key-auth (no token needed)
realtime round-trip through the proxy. `rest_host` is also accepted (the SDK
warns it is deprecated in favour of `endpoint`).

Usage:
    ABLY_PUBLIC_PORT=8583 python examples/ably_python_smoke.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path

from ably import AblyRealtime

PORT = int(os.environ.get("ABLY_PUBLIC_PORT", "8583"))
KEY = os.environ.get("ABLY_SERVER_API_KEY", "app.key:secret")
RESULTS = (
    Path(__file__).resolve().parent.parent.parent
    / "results"
    / "python-smoke.json"
)
TIMEOUT_S = 20.0
# host:port — the workaround for ably-python ignoring `port` on the WS transport.
HOSTPORT = f"127.0.0.1:{PORT}"


def _opts() -> dict:
    return dict(
        key=KEY,
        realtime_host=HOSTPORT,
        rest_host=HOSTPORT,
        tls=False,
        # The harness drives JSON; match it so any wire issue is apples-to-apples.
        use_binary_protocol=False,
        auto_connect=True,
    )


async def realtime_roundtrip() -> dict:
    """Realtime publish->subscribe round-trip via AblyRealtime."""
    client = AblyRealtime(**_opts())
    nonce = os.urandom(6).hex()
    payload = {"hello": "ably-python", "ts": time.time(), "nonce": nonce}

    got: "asyncio.Future[dict]" = asyncio.get_running_loop().create_future()

    await asyncio.wait_for(
        client.connection.once_async("connected"), timeout=TIMEOUT_S
    )
    channel = client.channels.get("embed-poc-python-smoke")

    def on_msg(message):
        data = message.data
        if isinstance(data, dict) and data.get("nonce") == nonce and not got.done():
            got.set_result(data)

    await channel.subscribe("greeting", on_msg)
    await channel.publish("greeting", payload)

    received = await asyncio.wait_for(got, timeout=TIMEOUT_S)
    await client.close()
    return {
        "transport": "realtime-websocket",
        "auth": "key (basic, over ws query string) — no token needed",
        "endpointWorkaround": (
            "ably-python 3.1.2 ignores `port` on the WS transport; "
            f"folded into realtime_host={HOSTPORT!r}"
        ),
        "roundTrip": True,
        "nonce": received.get("nonce"),
    }


def main() -> int:
    started = time.time()
    result: dict = {
        "label": "python-smoke",
        "sdk": "ably (python)",
        "endpoint": f"127.0.0.1:{PORT}",
        "tls": False,
        "startedAt": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        detail = loop.run_until_complete(realtime_roundtrip())
        result.update({"pass": True, **detail})
        print(
            f"SMOKE PASS: unmodified ably-python {detail['transport']} "
            f"round-trip through the proxy via {detail['auth']}"
        )
        rc = 0
    except Exception as exc:
        result.update(
            {"pass": False, "error": f"{type(exc).__name__}: {exc}"}
        )
        print(f"SMOKE FAIL: {type(exc).__name__}: {exc}", file=sys.stderr)
        rc = 1
    finally:
        result["wallMs"] = round((time.time() - started) * 1000, 2)

    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    RESULTS.write_text(json.dumps(result) + "\n")
    print(f"[smoke] wrote {RESULTS}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
