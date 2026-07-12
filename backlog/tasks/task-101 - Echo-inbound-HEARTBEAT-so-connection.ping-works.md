---
id: TASK-101
title: Echo inbound HEARTBEAT so connection.ping() works
status: Done
assignee:
  - '@claude'
created_date: '2026-07-12 13:58'
updated_date: '2026-07-12 16:01'
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
- [x] #1 connection.ping() resolves with a latency value (connectionPingWithCallback passes)
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add ActionHeartbeat case to connection.dispatch: reply with a HEARTBEAT ProtocolMessage echoing the inbound frame's ID (mirrors reference frontdoor connection.go).
2. Add an echo_test / dispatch test asserting an inbound HEARTBEAT with id yields a HEARTBEAT reply with the same id.
3. Verify against ably-js connection.test.js connectionPingWithCallback.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Root cause: connection.dispatch had no ActionHeartbeat case, so a client-initiated HEARTBEAT (connection.ping) was silently dropped; ably-js correlates the ping response by frame id (connectionmanager ping: resolves only when responseId === id), so the idle write-loop HEARTBEAT (no id) never satisfied it -> timeout.

Fix: added handleHeartbeat that replies with a HEARTBEAT echoing the inbound frame's ID, mirroring the reference frontdoor connection.go (id copied straight back, omitempty on the wire). Added TestHeartbeatEchoesID.

Before: connectionPingWithCallback FAILED ('Timeout waiting for heartbeat response'). After: PASS (1 passing, ~473ms).

Gates: go build/vet/vet-integration/test all green; go test -race ./internal/realtime/ green.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Echo inbound HEARTBEAT so connection.ping() resolves (TASK-101).

Root cause: connection.dispatch lacked an ActionHeartbeat case; client-initiated HEARTBEATs (connection.ping) were dropped. ably-js correlates the ping response by frame id, so the idle server HEARTBEAT (no id) never resolved the ping and it timed out.

Change: added connection.handleHeartbeat, which replies with a HEARTBEAT echoing the inbound frame's ID (mirrors the reference frontdoor handler). Added TestHeartbeatEchoesID.

Verification: ably-js realtime/connection connectionPingWithCallback now passes (was a timeout). All Go gates green including -race on internal/realtime.
<!-- SECTION:FINAL_SUMMARY:END -->
