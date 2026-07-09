---
id: TASK-84
title: Reconcile REST message-updates with the SDK (TestRESTChannel_MessageUpdates)
status: To Do
assignee: []
created_date: '2026-07-09 19:22'
labels:
  - rest
  - compat
dependencies:
  - TASK-53
priority: high
ordinal: 84000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-09.md gap 6: the realtime message-updates path passes but TestRESTChannel_MessageUpdates fails, despite REST mutations landing in TASK-53 (PATCH /channels/{channel}/messages/{serial}). Diagnose what the SDK actually sends for REST update/delete/append (route, method, body shape, response envelope) and reconcile the implementation — the mismatch is likely route or payload shape rather than missing functionality.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The SDK's REST mutation request shape is documented in the task notes
- [ ] #2 TestRESTChannel_MessageUpdates passes against a local server
<!-- AC:END -->
