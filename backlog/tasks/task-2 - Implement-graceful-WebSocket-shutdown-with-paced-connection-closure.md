---
id: TASK-2
title: Implement graceful WebSocket shutdown with paced connection closure
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 14:25'
updated_date: '2026-07-09 12:38'
labels:
  - ops
dependencies: []
ordinal: 2000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
On SIGTERM the server calls srv.Shutdown in cmd/ably-server/main.go, which stops new connections and drains idle HTTP, but never closes the hijacked WebSocket connections — they are dropped abruptly when the process exits at the --shutdown-grace deadline.

Per DESIGN.md §11, shutdown should send a DISCONNECTED frame (protocol action 6) to every live WebSocket so SDKs reconnect elsewhere, then force-close any stragglers after the grace window. Crucially, these closures should be spread evenly across the --shutdown-grace window rather than fired all at once, to avoid a thundering-herd reconnect storm against the next node.

This needs a registry of active connections on realtime.Server (internal/realtime/server.go): register in HandleWebSocket and deregister when conn.run returns. Add a Shutdown(ctx) method that walks the registry and paces DISCONNECTED frames evenly over the deadline, and wire it into the shutdown sequence in main.go alongside srv.Shutdown.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Connections are closed evenly across the --shutdown-grace window, not all at once
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add a connection registry to realtime.Server (mu + map[*connection]struct{}); register in HandleWebSocket before conn.run and deregister via defer when run returns.
2. Add connection.disconnect(): enqueue a DISCONNECTED (action 6) frame; the write loop, after writing a DISCONNECTED, closes the socket so the read loop exits and normal teardown runs (synthesising presence LEAVEs). If the outbound queue can't accept it (writer gone/full), force-close the ws directly.
3. Add Server.Shutdown(ctx): snapshot the registry and disconnect each connection paced evenly over the ctx deadline window (interval = window/N; disconnect at 0, interval, ... (N-1)*interval), force-closing any remaining stragglers immediately when the deadline is hit.
4. Wire into main.go SIGTERM path: run srv.Shutdown in a goroutine (stops accepting + drains REST) and call rt.Shutdown(shutdownCtx) concurrently to pace the WS closures, then await srv.Shutdown; keep debugSrv shutdown.
5. Tests: N connections each receive DISCONNECTED, closures spread across the window (span >> 0); a peer still sees a synthesised presence LEAVE for a member held by a shutdown connection (DESIGN §12.5). Confirm DESIGN §11 matches the implemented sequence.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
The write loop closes the socket after flushing a DISCONNECTED, guaranteeing the frame reaches the client before the close and that the normal read-loop teardown (presence LEAVE synthesis, §12.5) runs. Verified teardown LEAVEs still fire on the shutdown path (member drops from the presence set after Shutdown). main.go runs srv.Shutdown in a goroutine concurrently with rt.Shutdown so the blocking WebSocket handlers are unblocked by the paced closes. DESIGN §11 updated to describe the paced active-close sequence.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Graceful, paced WebSocket shutdown. realtime.Server now keeps a registry of live connections (register in HandleWebSocket, deregister when run() returns) and exposes Shutdown(ctx): it snapshots the registry and disconnects each connection spaced evenly over the ctx deadline window (interval = window/N), force-closing any straggler still open at the deadline. connection.disconnect() enqueues a DISCONNECTED (action 6, code 80003) frame; the write loop flushes it and then closes the socket, so the client receives the frame before the close and the read-loop teardown runs unchanged — including synthesised presence LEAVEs (DESIGN §12.5). Wired into the SIGTERM path in cmd/ably-server/main.go: srv.Shutdown runs in a goroutine (stops accepting + drains REST) while rt.Shutdown paces the WebSocket closes, then main awaits srv.Shutdown. Updated DESIGN.md §11 to describe the paced active-close sequence. Tests (under -race): N connections each receive DISCONNECTED with closures spread across the window (not all at once); a member's presence LEAVE is synthesised via the shutdown close path (member drops from the set after Shutdown).
<!-- SECTION:FINAL_SUMMARY:END -->
