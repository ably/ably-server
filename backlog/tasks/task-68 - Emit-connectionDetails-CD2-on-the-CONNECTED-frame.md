---
id: TASK-68
title: Emit connectionDetails (CD2) on the CONNECTED frame
status: To Do
assignee: []
created_date: '2026-06-30 18:58'
labels:
  - embedding-poc
  - sdk-compat
dependencies: []
references:
  - EMBEDDING-POC.md
  - internal/realtime/connection.go
ordinal: 68000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Surfaced by the embedding PoC (M2): the unmodified ably-js SDK treats a CONNECTED frame without connectionDetails as a protocol error and refuses the connection. ably-go and the .NET IO.Ably SDK tolerate its absence. A working implementation lives on branch embed-poc-m2-node (protocol.ConnectionDetails + populated on connect).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 CONNECTED carries connectionDetails with at least connectionKey and maxIdleInterval
- [ ] #2 unmodified ably-js reaches CONNECTED and completes a pub/sub round-trip
- [ ] #3 existing realtime/protocol tests still pass
<!-- AC:END -->
