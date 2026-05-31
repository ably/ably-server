---
id: TASK-25
title: Add multi-server cluster integration test (testcontainer + N servers)
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 16:27'
updated_date: '2026-05-31 22:24'
labels: []
dependencies:
  - TASK-22
  - TASK-23
ordinal: 25000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Add an integration test (same `integration` build tag) that brings up one PostgreSQL testcontainer plus multiple ably-server instances in cluster mode, then verifies cross-node pub/sub: a publish sent through each server reaches subscribers attached to every other server (full mesh), with correct ordering and no duplicates. This exercises the LISTEN/NOTIFY broker end to end across nodes. Depends on the cluster pub/sub broker and auto-migrate.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 startServer is refactored into startServerOnDSN(t, dsn) (low-level) + startServer(t) (single-node convenience that does FreshSchemaDSN + startServerOnDSN). Multi-node tests get one schema and pass it to every server.
- [x] #2 TestIntegrationClusterFullMesh boots N=3 ably-server instances against one shared Postgres schema, each on its own ephemeral port, connects one ably-go SDK client per server, attaches each to the same channel, then publishes one message via each WS in sequence. Asserts each of the three clients observes all three messages exactly once, with no duplicates and no extra messages.
- [x] #3 TestIntegrationClusterRESTPublishObservedAcrossNodes boots two servers on one shared schema; subscribes a WS client on server B; publishes via REST to server A; asserts B's subscriber observes the cm. Proves cross-node delivery for the REST-publish + WS-subscribe combination.
- [x] #4 Ordering: TestIntegrationClusterFullMesh collects the order each client observes the cms and asserts all clients see them in the same order (per DESIGN §7.2's commit-order delivery from a single PG).
- [x] #5 All synchronisation is channel-based + ctx-bounded via testCtx; the 'no extras' check uses a bounded drain (200ms) after the expected N messages — no magic-number sleep gating the main assertion.
- [x] #6 go build, go vet, go test ./..., go test -race ./... remain clean without the integration tag. go test -tags=integration ./... runs the new tests against the warm pgtest testcontainer.
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Refactor startServer in cmd/ably-server/integration_test.go:
   - Extract the run+ready+cleanup machinery into startServerOnDSN(t, dsn).
   - startServer(t) becomes a single-node convenience: pgtest.Start + FreshSchemaDSN + startServerOnDSN.

2. Add TestIntegrationClusterFullMesh:
   - Bring up pgtest.Container once; one FreshSchemaDSN; start 3 servers via startServerOnDSN.
   - Connect 3 ably-go SDK clients, one per server.
   - SubscribeAll on channel 'mesh' from each; each handler pushes onto its own buffered chan.
   - For i := 0..2: clients[i].Channels.Get('mesh').Publish(ctx, fmt.Sprintf('from-%d', i), …).
   - For each client: read exactly N messages, assert each from-N appears exactly once, then drain-with-bounded-timeout asserts no extras.
   - Collect the order each client observed; assert all three orders are identical (commit-order consistency).

3. Add TestIntegrationClusterRESTPublishObservedAcrossNodes:
   - Two servers, one shared schema.
   - SDK client on B subscribes to 'cross'.
   - REST POST to A's /channels/cross/messages.
   - B's subscriber receives within testCtx; assert payload.

4. Run go build, go vet, go test ./..., go test -race ./..., go test -tags=integration ./... — all clean.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
All three nodes share one PG schema (one FreshSchemaDSN, three startServerOnDSN calls). Each server's own LISTEN goroutine fires its registered Appender from the NOTIFY round-trip — no self-dedup. Concurrent migrate at boot is safe (TASK-23's advisory lock).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Multi-server cluster integration test: three ably-server instances sharing one Postgres schema verify full-mesh cross-node fan-out and order consistency via the LISTEN/NOTIFY broker.

## What's in

- cmd/ably-server/integration_test.go gains:
  - startServerOnDSN(t, dsn): extracted from startServer so multi-node tests can boot N servers against one shared FreshSchemaDSN. startServer is now a single-node convenience that calls FreshSchemaDSN + startServerOnDSN.
  - TestIntegrationClusterFullMesh: 3 servers, 3 SDK clients (one per server), all attached to channel 'mesh'. Each client publishes once via its own WS; every client must observe all three messages, exactly once, in the same order. Proves cross-node fan-out + commit-order consistency.
  - TestIntegrationClusterRESTPublishObservedAcrossNodes: 2 servers, REST publish to A is observed by WS subscriber on B. Proves the cross-protocol cross-node path.

## How it works

One pgtest.Container, one FreshSchemaDSN, multiple startServerOnDSN calls. Each ably-server boots independently:
- Concurrent migrate at startup is safe (TASK-23's advisory lock).
- Each has its own pgxpool + dedicated LISTEN conn + its own seriesId.
- The publish path goes through the unified Appender flow: Store → NOTIFY → every node's LISTEN goroutine → Appender → local Channel. No self-dedup.

## Assertions

- All N clients observe exactly N messages (no missing, no duplicates).
- The drain-with-bounded-timeout (200ms) catches duplicates that would arrive late.
- Order consistency: orders[i] == publish order for every i. This holds because the NOTIFY delivery order on every listener matches PG's commit order — which is the publish sequence in our sequential test.

## Verification

- go build, go vet — clean.
- go test ./..., go test -race ./... — clean.
- go test -tags=integration ./... — passes. The new TestIntegrationClusterFullMesh runs in ~1.9s (booting 3 servers + 3 SDK clients sequentially); RESTPublishObservedAcrossNodes in ~60ms.

## Limitation explicitly accepted

The 'no extras' drain has a 200ms bound. A lurking duplicate that arrives >200ms after the expected N messages would not be caught — but the unified flow has no source for such duplicates (the publisher's NOTIFY arrives at every node exactly once via PG's commit-order delivery). If TASK-29's reconnect-and-reconcile work introduces a duplicate-emission risk, that test should be updated alongside.

## What this proves end-to-end

The complete TASK-25 chain: TASK-13 → TASK-21 → TASK-22 → TASK-23 → TASK-30 → TASK-24 → TASK-25. Three actual ably-server processes (well, run() goroutines) sharing a real Postgres database deliver each other's publishes correctly via LISTEN/NOTIFY. The dependency tree at the top of the conversation is now fully discharged.
<!-- SECTION:FINAL_SUMMARY:END -->
