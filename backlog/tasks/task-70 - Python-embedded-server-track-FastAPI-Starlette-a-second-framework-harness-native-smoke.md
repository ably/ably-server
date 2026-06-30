---
id: TASK-70
title: >-
  Python embedded-server track (FastAPI/Starlette + a second framework) +
  harness + native smoke
status: Done
assignee:
  - '@claude'
created_date: '2026-06-30 20:43'
updated_date: '2026-06-30 21:36'
labels:
  - embedding-poc
dependencies: []
references:
  - EMBEDDING-POC.md
ordinal: 70000
---

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Python host supervises the ably-server child and reverse-proxies WS+REST on a dedicated port (ASGI)
- [x] #2 Go conformance harness passes through the Python proxy; fault injection run (kill-9 restart, clean shutdown)
- [x] #3 native ably (python) SDK does a pub/sub round-trip through the proxy; a second framework covered
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Python track merged + coordinator-verified. FastAPI/Starlette ASGI host supervises the ably-server child and reverse-proxies REST+WebSocket via a catch-all ASGI app (httpx + websockets), root or /ably subpath, serving /demo/. Harness PASS all six at root + PASS under --base-path /ably; unmodified ably-python realtime round-trips through the proxy (key auth, no token); fault injection passes (kill-9 respawn; SIGTERM exit 0, no orphan); a spawned code-review independently stress-tested the supervisor race + WS teardown (0 orphans). Findings: WSGI (Flask/sync Django) proxies REST but not WS in-process; ably-python ignores ClientOptions.port on the WS transport (fold into realtime_host).
<!-- SECTION:FINAL_SUMMARY:END -->
