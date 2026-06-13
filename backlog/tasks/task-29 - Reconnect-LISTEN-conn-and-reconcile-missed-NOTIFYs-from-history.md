---
id: TASK-29
title: Reconnect LISTEN conn and reconcile missed NOTIFYs from history
status: To Do
assignee: []
created_date: '2026-05-31 21:58'
updated_date: '2026-06-03 13:06'
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
- [ ] #1 postgres.Storage's LISTEN goroutine no longer exits on WaitForNotification errors. It logs, sleeps a backoff window, re-dials a fresh pgx.Conn, re-execs LISTEN, and resumes — bounded reconnect retry until the storage is Close()d.
- [ ] #2 Each registered channelStore tracks the last channel_serial it has Appended (updated atomically inside the LISTEN dispatch). On reconnect, the broker calls History(ctx, HistoryQuery{AfterChannelSerial: lastSeen}) per channel and Appends each returned cm before re-entering the WaitForNotification loop.
- [ ] #3 Reconciliation is idempotent against the normal NOTIFY path: a cm that lands via both history-replay-on-reconnect AND a subsequent NOTIFY is delivered to the appender exactly once. (Most likely via a per-channel last-seen high-water mark; the Append call site skips cms whose serial <= lastSeen.)
- [ ] #4 Integration test (//go:build integration): bring up postgres testcontainer + a postgres.Storage with a recording appender; publish cms; force-close the listenConn from outside (or inject a controlled drop); publish more cms during the gap; verify the recording appender observes ALL cms (pre-gap + during-gap + post-gap) in order, each exactly once.
- [ ] #5 DESIGN.md §7.2 updated to describe the reconnect + reconcile path; the 'follow-up' caveat removed.
<!-- AC:END -->
