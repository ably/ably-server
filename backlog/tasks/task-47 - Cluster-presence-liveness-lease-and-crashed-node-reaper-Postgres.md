---
id: TASK-47
title: Cluster presence liveness lease and crashed-node reaper (Postgres)
status: To Do
assignee: []
created_date: '2026-06-13 09:38'
updated_date: '2026-06-14 19:40'
labels:
  - cluster
dependencies:
  - TASK-43
  - TASK-44
  - TASK-22
documentation:
  - DESIGN.md
ordinal: 47000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Handle presence orphans left by a crashed cluster node per DESIGN.md section 12.5. A node that dies without running teardown cannot emit its LEAVEs, orphaning rows in the postgres presence table (a cluster-only problem; a single-process crash takes the whole set down with it). Add node_id and expires_at columns to the presence table (forward-only migration). StorePresence stamps the owning node_id and an expires_at lease on ENTER/UPDATE; each live node bumps expires_at for all of its rows on the heartbeat tick. A periodic reaper runs DELETE FROM presence WHERE expires_at past now RETURNING the deleted rows, and for each reaped member synthesises a LEAVE through the normal publish path so subscribers on every node observe the departure. Postgres row locking ensures exactly one node reaps and emits the LEAVE for a given row. Orphan visibility is bounded to one lease window.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A forward-only migration adds presence.node_id and presence.expires_at; auto-migrate applies it on startup
- [ ] #2 StorePresence sets node_id to the owning node and expires_at to now plus the lease on ENTER and UPDATE
- [ ] #3 Each node periodically bumps expires_at for all rows it owns in a single statement on the heartbeat cadence
- [ ] #4 A reaper deletes rows past expires_at and emits a synthetic LEAVE for each through the normal publish path; exactly one node emits the LEAVE for a given row
- [ ] #5 After a node is killed without teardown, its members disappear from Members and a LEAVE reaches other nodes within one lease window
- [ ] #6 The lease window and reaper cadence are defined as constants; making them configurable is a follow-up
- [ ] #7 A multi-node testcontainer test verifies orphan reaping after a simulated node death
- [ ] #8 Multi-node integration test: a member whose owning node is killed is reaped — removed from Members and a synthetic LEAVE propagates to other nodes within a lease window (the crashed-node case deferred from TASK-48)
<!-- AC:END -->
