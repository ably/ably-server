---
id: TASK-48
title: Add multi-server cluster integration test for presence
status: Done
assignee:
  - '@lmars'
created_date: '2026-06-13 09:38'
updated_date: '2026-06-14 19:40'
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
- [x] #1 A client attaching on node B receives node A members via SYNC
- [x] #2 ENTER, UPDATE and LEAVE published on one node are delivered to PRESENCE_SUBSCRIBE subscribers on another node
- [x] #3 Members returns a consistent set across all nodes after a sequence of presence ops
- [x] #4 The test is build-tagged (integration) and runs in CI alongside the existing cluster tests
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Multi-server cluster integration test for presence, driven by the ably-go SDK against a Postgres testcontainer (DESIGN.md §12.4, §12.5, §7.2).

Two tests in cmd/ably-server/presence_integration_test.go (build tag: integration):
- TestIntegrationClusterPresenceSyncAcrossNodes — alice enters on node A; a client on node B sees her via cross-node SYNC (AC #1); after bob enters on B, both nodes converge to {alice, bob} (AC #3).
- TestIntegrationClusterPresenceEventsAcrossNodes — a subscriber on node B receives alice's enter/update/leave, in order, published on node A via the LISTEN/NOTIFY broker (AC #2).

Verified end-to-end SDK interop: ably-go's presence sync state machine accepts our SYNC frame (HAS_PRESENCE flag on ATTACHED; members keyed by connectionId+clientId; the '<serial>:' empty-cursor channelSerial marks the set complete), and our cross-node delivery feeds its presence map. newClientWithID adds WithClientID so the SDK sends ?clientId= (RSA7e1, Basic auth) which the server resolves per §3.2; newClient now delegates to it. Added waitPresenceSet (polls Presence.Get until the set matches, tolerating propagation latency).

Both tests pass against Docker Postgres; the full cmd integration suite stays green.

Out of scope: the crashed-node reaping case (original AC #4) depends on the TASK-47 lease/reaper and has been moved to TASK-47.
<!-- SECTION:FINAL_SUMMARY:END -->
