---
id: TASK-19
title: Set ConnectionDetails on the outbound CONNECTED message
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 16:11'
updated_date: '2026-07-09 12:16'
labels:
  - protocol
dependencies:
  - TASK-11
ordinal: 19000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Populate a ConnectionDetails on the CONNECTED ProtocolMessage (the first frame after upgrade, DESIGN §2.1/§8). Include the resolved clientId (§3.2), the connectionId, and the connection limits/params SDKs expect (e.g. maxMessageSize, maxFrameSize, connectionStateTtl, maxIdleInterval / server heartbeat cadence). Likely requires adding a ConnectionDetails type to internal/protocol and a field on ProtocolMessage. Depends on clientId resolution (ConnectionDetails carries the resolved clientId).
<!-- SECTION:DESCRIPTION:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add protocol.ConnectionDetails (clientId, connectionKey, maxMessageSize, maxFrameSize, maxInboundRate, connectionStateTtl, maxIdleInterval; json+msgpack tags matching ably-go's connectionDetails; durations as int64 ms) and a ConnectionDetails *ConnectionDetails field on ProtocolMessage.
2. Populate it on the outbound CONNECTED frame in connection.go: clientId=resolved c.clientID (omitempty drops anonymous, sends '*' for wildcard), connectionKey=c.id (connection-state resume is a non-goal, so the key is the opaque connectionId), maxIdleInterval=heartbeatInterval. Defaults via named constants: maxMessageSize 65536, maxFrameSize 524288, maxInboundRate 1000 (advisory), connectionStateTtl 120000ms.
3. Document the fields/defaults in DESIGN.md §2.1 and §8.
4. Unit test asserting CONNECTED carries ConnectionDetails with the resolved clientId, connectionKey==connectionId, and the limits (incl. maxIdleInterval aligned to the test heartbeat).
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
connectionDetails.clientId sends the resolved identity verbatim (concrete / '*' / omitted-when-anonymous); connectionKey reuses the connectionId since connection-state resume is a non-goal; maxIdleInterval = server heartbeat cadence. Limits are advisory (not enforced yet).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Populate ConnectionDetails on the CONNECTED frame. Added protocol.ConnectionDetails (field names + json/msgpack tags matching ably-go's connectionDetails) and a ConnectionDetails field on ProtocolMessage. The realtime connection now advertises, on CONNECTED: the resolved clientId (§3.2 — concrete value, '*' for wildcard, omitted for anonymous), connectionKey (the opaque, non-resumable connectionId), maxMessageSize=65536, maxFrameSize=524288, maxInboundRate=1000 (advisory), connectionStateTtl=120000ms, and maxIdleInterval aligned to the server heartbeat cadence. Documented the fields/defaults in DESIGN.md §2.1 and §8. Tests assert the CONNECTED frame carries these for concrete and wildcard clientIds. No ACs were defined on this task.
<!-- SECTION:FINAL_SUMMARY:END -->
