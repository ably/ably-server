---
id: TASK-19
title: Set ConnectionDetails on the outbound CONNECTED message
status: To Do
assignee: []
created_date: '2026-05-31 16:11'
updated_date: '2026-06-03 13:06'
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
