---
id: TASK-35
title: Decide scope for a channel lifecycle/status REST endpoint
status: To Do
assignee: []
created_date: '2026-06-01 10:04'
updated_date: '2026-06-03 13:06'
labels:
  - scoping
dependencies: []
ordinal: 35000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
ably-go TestGetChannelLifecycleStatus_RSL8 fails: there is no channel metadata/status (occupancy, lifecycle) REST endpoint. This is likely a non-goal alongside the other Ably-cloud product-surface APIs. Decision needed: implement a minimal channel-status endpoint, or document as a non-goal in DESIGN.md and close.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Decision recorded: channel status endpoint is in scope or an explicit non-goal
<!-- AC:END -->
