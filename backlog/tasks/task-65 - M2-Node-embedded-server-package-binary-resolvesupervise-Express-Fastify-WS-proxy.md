---
id: TASK-65
title: >-
  M2: Node embedded-server package: binary resolve+supervise + Express/Fastify
  WS proxy
status: Done
assignee:
  - '@claude'
created_date: '2026-06-30 16:31'
updated_date: '2026-06-30 18:58'
labels:
  - embedding-poc
dependencies: []
references:
  - EMBEDDING-POC.md
ordinal: 65000
---

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 spawns binary on a free port, health-checks /readyz, restarts on crash, shuts down with app
- [x] #2 Express and Fastify middleware proxy realtime traffic incl. WebSocket
- [x] #3 harness passes through an example Node app; metrics recorded
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Express + Fastify adapters over a supervised ably-server child (free port, /readyz, restart-on-crash, stop-with-app). Coordinator independently verified: harness PASS 6/6 through BOTH proxies; native ably-js smoke PASS through both; restart-on-crash verified with exact-PID tracking (86591 -> SIGKILL -> respawn 86615 -> harness PASS); graceful shutdown exits 0 in ~0.1s with no orphan. Found+fixed a shutdown hang (must stop child before closing server). Surfaced two server SDK-compat fixes for ably-js: connectionDetails (CD2) and msgSerial *int64 (TASK-68, TASK-69).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Node embedded-server package (primary track): supervise a prebuilt ably-server child + reverse-proxy realtime+REST to it on a dedicated port, via Express (http-proxy-middleware) and Fastify (@fastify/http-proxy) adapters.

Verified (coordinator re-ran from clean): Go harness PASS 6/6 through Express AND Fastify (resume + 200-msg zero-loss soak incl.); unmodified ably-js does a pub/sub round-trip through both; kill -9 child auto-restarts (restart attempt logged, re-ready, harness passes); SIGTERM exits 0 in ~0.1s, no orphan.

Found + fixed a real graceful-shutdown hang: closing the HTTP server before stopping the child hangs after traffic (lingering proxied socket; ably-js also reconnects) — fix is stop-child-first.

Proving the UNMODIFIED ably-js SDK required two server fixes (ably-go/.NET tolerate the gaps): emit connectionDetails/CD2 on CONNECTED, and make msgSerial *int64 so msgSerial=0 serialises in ACKs (else ably-js publish hangs). Filed as TASK-68 and TASK-69; committed separately on this branch; full server test suite green.
<!-- SECTION:FINAL_SUMMARY:END -->
