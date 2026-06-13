---
id: TASK-48
title: Add multi-server cluster integration test for presence
status: To Do
assignee: []
created_date: '2026-06-13 09:38'
labels:
  - cluster
dependencies:
  - TASK-44
  - TASK-45
  - TASK-47
documentation:
  - DESIGN.md
ordinal: 48000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Add a multi-server integration test for presence per DESIGN.md sections 12.4, 12.5 and 7.2, extending the existing cluster test harness (testcontainer plus N servers, as in TASK-25). Cover: a member entering on node A is visible via sync to a client attaching on node B; enter/update/leave on one node reach PRESENCE_SUBSCRIBE subscribers on another via the NOTIFY path; the membership set in Members is consistent across nodes after a sequence of ops; and a member whose owning node dies is reaped and a LEAVE propagates to other nodes within a lease window. Build-tagged like the other integration tests.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A client attaching on node B receives node A members via SYNC
- [ ] #2 ENTER, UPDATE and LEAVE published on one node are delivered to PRESENCE_SUBSCRIBE subscribers on another node
- [ ] #3 Members returns a consistent set across all nodes after a sequence of presence ops
- [ ] #4 A member whose owning node is killed is removed from the set and a LEAVE propagates to other nodes within a lease window
- [ ] #5 The test is build-tagged (integration) and runs in CI alongside the existing cluster tests
<!-- AC:END -->
