---
id: TASK-2
title: Implement graceful WebSocket shutdown with paced connection closure
status: To Do
assignee: []
created_date: '2026-05-31 14:25'
updated_date: '2026-06-03 13:06'
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
- [ ] #1 Connections are closed evenly across the --shutdown-grace window, not all at once
<!-- AC:END -->
