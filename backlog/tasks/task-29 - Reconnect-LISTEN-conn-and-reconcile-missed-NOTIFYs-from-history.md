---
id: TASK-29
title: Reconnect LISTEN conn and reconcile missed NOTIFYs from history
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 21:58'
updated_date: '2026-07-09 13:39'
labels:
  - ops
dependencies:
  - TASK-22
ordinal: 29000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Postgres LISTEN delivers notifications at-most-once during disconnects: if the dedicated LISTEN connection drops, NOTIFYs emitted in the gap are not redelivered when it reconnects. The current broker treats a WaitForNotification error as fatal and stops the loop.

Make the broker resilient: detect the disconnect, reconnect with backoff, and on resume reconcile per channel by reading history past the last-seen channelSerial — calling appender.Append(cm) for each missing entry. This is what brings cluster pub/sub from 'works under steady state' to 'survives PG restarts and transient network blips'.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 postgres.Storage's LISTEN goroutine no longer exits on WaitForNotification errors. It logs, sleeps a backoff window, re-dials a fresh pgx.Conn, re-execs LISTEN, and resumes — bounded reconnect retry until the storage is Close()d.
- [x] #2 Each registered channelStore tracks the last channel_serial it has Appended (updated atomically inside the LISTEN dispatch). On reconnect, the broker calls History(ctx, HistoryQuery{AfterChannelSerial: lastSeen}) per channel and Appends each returned cm before re-entering the WaitForNotification loop.
- [x] #3 Reconciliation is idempotent against the normal NOTIFY path: a cm that lands via both history-replay-on-reconnect AND a subsequent NOTIFY is delivered to the appender exactly once. (Most likely via a per-channel last-seen high-water mark; the Append call site skips cms whose serial <= lastSeen.)
- [x] #4 Integration test (//go:build integration): bring up postgres testcontainer + a postgres.Storage with a recording appender; publish cms; force-close the listenConn from outside (or inject a controlled drop); publish more cms during the gap; verify the recording appender observes ALL cms (pre-gap + during-gap + post-gap) in order, each exactly once.
- [x] #5 DESIGN.md §7.2 updated to describe the reconnect + reconcile path; the 'follow-up' caveat removed.
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add AfterChannelSerial (strict lower bound on channel_serial) to storage.HistoryQuery; implement the predicate in postgres, memory and bbolt History (honours the already-documented contract).
2. Give postgres channelStore a mutex-guarded lastSeen high-water mark; route all appends through cs.deliver(cm) which skips cms with serial <= lastSeen, advances the mark, then calls appender.Append. Both the NOTIFY dispatch and reconcile go through deliver, so dedup is idempotent.
3. Restructure listenLoop: consume() runs WaitForNotification until error; on a non-cancel error close the conn, back off (capped exponential, test-tunable package vars), re-dial a fresh pgx.Conn, re-LISTEN, reconcile, resume. Loop until ctx (Close) cancels.
4. reconcile: for each registered channelStore, History(DirectionForwards, AfterChannelSerial=lastSeen) for both KindMessage and KindPresence, merge by ChannelSerial ascending, deliver each. LISTEN precedes reconcile so no gap.
5. Close cancels ctx and waits; listenLoop owns closing the conn.
6. Integration test (package postgres, integration tag): force-close the listen conn mid-stream, publish during the gap, assert the recording appender sees all cms exactly once in order.
7. DESIGN.md §7.2: document reconnect+reconcile, remove the follow-up caveat.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Added AfterChannelSerial (strict channel_serial lower bound) to storage.HistoryQuery, implemented in postgres/memory/bbolt (honours the pre-existing but unimplemented contract doc).

postgres broker: listenLoop now consume()->redial()->reconcile() loop; capped-exponential backoff via package vars (test-tunable); conn ownership moved into the goroutine, Close cancels+waits via WaitGroup.

Per-channel high-water mark (lastSeen) with deliver() dedup; reconcileFromHistory merges message+presence History(AfterChannelSerial) streams by channelSerial.

Integration test forces the LISTEN backend down via pg_terminate_backend (scoped by application_name), publishes across the gap, asserts exactly-once in-order delivery. Passes under -race.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Make the Postgres cluster broker survive dropped LISTEN connections and reconcile the gap.

Problem: the LISTEN goroutine treated any WaitForNotification error as fatal and exited, so a dropped LISTEN conn (PG restart, network blip) silently stopped all cross-node delivery on that node, and NOTIFYs emitted during the gap were lost (PG delivers at-most-once).

Changes:
- storage.HistoryQuery gains AfterChannelSerial, a strict channel_serial lower bound (the History interface already documented it). Implemented in the postgres, memory and bbolt backends.
- postgres.Storage: the LISTEN goroutine now loops consume -> redial -> reconcile. On a non-cancel WaitForNotification error it logs, closes the conn, re-dials a fresh pgx.Conn with capped exponential backoff, re-LISTENs, reconciles, and resumes — retrying until Close(). Conn ownership moved into the goroutine; Close() cancels a context and waits on a WaitGroup.
- Per-channel high-water mark (lastSeen): every append (steady-state NOTIFY dispatch and reconcile replay) funnels through channelStore.deliver, which drops any cm whose serial is not strictly greater than the last delivered. reconcileFromHistory replays History(AfterChannelSerial=lastSeen) for both the message and presence streams, merged in channelSerial order. Re-LISTEN precedes the reconcile scan, so a cm committed mid-reconcile is still observed via a buffered NOTIFY, deduped by the high-water mark — exactly once, in order.
- DESIGN.md §7.2 rewritten to describe the reconnect+reconcile path; the 'follow-up' caveat removed.

Tests: new integration test (whitebox, -tags=integration) force-terminates the LISTEN backend via pg_terminate_backend (scoped by application_name), publishes pre-gap/in-gap/post-gap, and asserts the recording appender sees all cms exactly once in order. Backoff constants are test-tunable package vars so it runs in ~2.5s. Full postgres integration suite and storage unit tests pass under -race.
<!-- SECTION:FINAL_SUMMARY:END -->
