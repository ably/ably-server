---
id: TASK-47
title: Cluster presence liveness lease and crashed-node reaper (Postgres)
status: Done
assignee:
  - '@claude'
created_date: '2026-06-13 09:38'
updated_date: '2026-07-09 13:46'
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
- [x] #1 A forward-only migration adds presence.node_id and presence.expires_at; auto-migrate applies it on startup
- [x] #2 StorePresence sets node_id to the owning node and expires_at to now plus the lease on ENTER and UPDATE
- [x] #3 Each node periodically bumps expires_at for all rows it owns in a single statement on the heartbeat cadence
- [x] #4 A reaper deletes rows past expires_at and emits a synthetic LEAVE for each through the normal publish path; exactly one node emits the LEAVE for a given row
- [x] #5 After a node is killed without teardown, its members disappear from Members and a LEAVE reaches other nodes within one lease window
- [x] #6 The lease window and reaper cadence are defined as constants; making them configurable is a follow-up
- [x] #7 A multi-node testcontainer test verifies orphan reaping after a simulated node death
- [x] #8 Multi-node integration test: a member whose owning node is killed is reaped — removed from Members and a synthetic LEAVE propagates to other nodes within a lease window (the crashed-node case deferred from TASK-48)
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Forward-only migration 0006_presence_liveness.sql adds presence.node_id and presence.expires_at (0004 explicitly deferred them to this task). Update the migrate-concurrency test's expected version list.
2. Storage grows a per-process node id; channelStore carries it. StorePresence stamps node_id and expires_at = now() + lease on ENTER/UPDATE/PRESENT.
3. A lease-bump loop (ticker) bumps expires_at for all rows this node owns in one UPDATE ... WHERE node_id = $1 on a cadence shorter than the lease window.
4. A reaper loop runs DELETE FROM presence WHERE expires_at < now() RETURNING channel, connection_id, client_id; for each reaped row it synthesises a LEAVE and publishes it through a transient channelStore.StorePresence (the normal path -> NOTIFY -> every node's appender). DELETE ... RETURNING row locking gives exactly-one emission.
5. Lease window, bump cadence and reaper cadence are package-level constants (test-tunable vars); operator config is a follow-up.
6. Multi-node integration test: member enters on node A, node A is Close()d (crash sim: stops its lease bump), node B reaps the orphan within a lease window — gone from Members and a synthetic LEAVE reaches B's appender exactly once.
7. Update DESIGN.md §12.5 to match the implemented mechanics (self-contained bump ticker in the backend rather than literally the WS heartbeat).
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Migration 0006_presence_liveness.sql adds node_id + expires_at (forward-only, with defaults so ADD COLUMN succeeds on a populated table); 0004 explicitly deferred these. Updated the migrate-concurrency test's expected version list.

Per-process node id on Storage/channelStore. StorePresence stamps node_id + expires_at = now() + make_interval(lease) on ENTER/UPDATE/PRESENT.

Two backend background loops (own tickers, cancelled by Close's context + WaitGroup): lease-bump (UPDATE ... WHERE node_id=$1) and reaper (DELETE ... WHERE expires_at < now() RETURNING). Reaper drains rows then emits a LEAVE per orphan via a transient channelStore.StorePresence (normal path -> NOTIFY). DELETE...RETURNING row lock = exactly-one emission.

Close made idempotent (sync.Once). Multi-node test: enter on A, Close(A) (crash sim), B reaps within a lease window — gone from Members and exactly one synthetic LEAVE at B's appender. Timing constants are test-tunable package vars (shrunk to 1s/200ms in the test); passes under -race.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Add a presence liveness lease and a crashed-node reaper so a cluster node that dies without teardown no longer orphans presence rows (DESIGN.md §12.5).

Problem: a node that crashes cannot emit its members' LEAVEs, leaving stale rows in the Postgres presence table that never fold out of the membership set — a cluster-only failure (a single-process crash takes its whole in-memory set down with it).

Changes:
- Migration 0006_presence_liveness.sql adds presence.node_id and presence.expires_at (forward-only, defaulted so the ADD COLUMN succeeds against a populated table; 0004 had deferred these to this task). Applied by the existing auto-migrate sweep.
- StorePresence stamps the owning node_id and a fresh lease (expires_at = now() + lease) on every ENTER/UPDATE/PRESENT upsert.
- A lease-bump loop refreshes expires_at for all rows this node owns in a single UPDATE ... WHERE node_id = $node on a cadence well inside the lease window, so a live node's members never lapse.
- A reaper loop runs DELETE FROM presence WHERE expires_at < now() RETURNING …, then synthesises a LEAVE for each reaped member through a transient channelStore.StorePresence — the normal publish path (fresh serial + presence cm insert + NOTIFY), so the departure reaches every node's appender. The DELETE ... RETURNING row lock guarantees exactly one node emits the LEAVE for a given row when several reap concurrently. Both loops run inside postgres.Storage, cancelled by Close's context + WaitGroup; Close is now idempotent.
- Lease window, bump cadence and reaper cadence are package-level constants (operator config is a follow-up).
- DESIGN.md §12.5 updated to describe the implemented mechanics (self-contained backend bump/reaper tickers rather than the WS heartbeat).

Tests: new multi-node testcontainer test (-tags=integration) enters a member on node A, Close()s A to simulate a crash (stopping its lease bump), and verifies node B reaps the orphan within a lease window — removed from Members and a synthetic LEAVE observed at B's appender exactly once. Timing constants are test-tunable package vars (shrunk to 1s / 200ms) so it runs in ~3s. Full postgres integration suite and go build/vet/test pass under -race.

Note: AC #4's exactly-one-emission is verified single-node in the test and guaranteed cross-node by the DELETE ... RETURNING row lock (concurrent-reap exclusivity is a Postgres property, not separately raced in a test).
<!-- SECTION:FINAL_SUMMARY:END -->
