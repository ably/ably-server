---
id: TASK-55
title: 'Mutable-messages integration tests: SDK round-trip and cross-node'
status: Done
assignee: []
created_date: '2026-06-13 14:46'
updated_date: '2026-06-14 22:11'
labels:
  - cluster
dependencies:
  - TASK-52
  - TASK-53
documentation:
  - DESIGN.md
ordinal: 55000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Per DESIGN.md sections 13.2, 13.4 and 7.2. Add integration coverage for mutable messages, build-tagged like the other integration tests. SDK compatibility: drive update/delete and version reads through ably-go and assert the wire shape (stable serial across versions, version object, action values, collapsed history vs getMessageVersions). Cluster: extending the multi-server harness (TASK-25), assert that an update/delete published on one node reaches subscribers on another via the NOTIFY path and that the materialised messages projection and collapsed history are consistent across nodes.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 update/delete and version reads round-trip through the ably-go SDK with the expected wire shape (stable serial, version object, action values)
- [x] #2 A mutation published on one cluster node is delivered to subscribers on another node in stream order
- [x] #3 Collapsed history and single-message reads are consistent across nodes after a sequence of mutations
- [x] #4 The tests are build-tagged (integration) and run in CI alongside the existing SDK and cluster tests
<!-- AC:END -->
