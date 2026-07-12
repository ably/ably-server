---
id: TASK-101
title: Echo inbound HEARTBEAT so connection.ping() works
status: To Do
assignee: []
created_date: '2026-07-12 13:58'
labels:
  - compat
  - ably-js
dependencies: []
priority: medium
ordinal: 101000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
ably-js connection.ping() sends a HEARTBEAT ProtocolMessage (with an id) and waits for a HEARTBEAT back; the server's dispatch (internal/realtime/connection.go:268-296) has no ActionHeartbeat case, so the frame is silently dropped and ping times out (realtime/connection connectionPingWithCallback: 'Timeout waiting for heartbeat response'). The server already emits HEARTBEAT on idle from the write loop; it just never responds to a client-initiated one. Fix: on inbound HEARTBEAT, reply with HEARTBEAT echoing the frame's id.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 connection.ping() resolves with a latency value (connectionPingWithCallback passes)
<!-- AC:END -->
