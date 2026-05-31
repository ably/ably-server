---
id: TASK-11
title: Resolve and stamp clientId (from JWT claim or URL param)
status: To Do
assignee: []
created_date: '2026-05-31 16:05'
updated_date: '2026-05-31 16:11'
labels: []
dependencies:
  - TASK-9
ordinal: 11000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Implement clientId resolution per DESIGN.md §3.2: derive the connection/request clientId from the x-ably-clientId JWT claim, or from the clientId query param when a basic API key is used, following the full resolution table in §3.2 — including the `*` wildcard (bearer may assume any clientId, but `*` is never itself a clientId) and every reject case. Stamp the resolved clientId onto every outbound Message.clientId for that connection, and NACK inbound messages that assert a different clientId. Depends on JWT auth.
<!-- SECTION:DESCRIPTION:END -->
