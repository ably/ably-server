---
id: TASK-33
title: Fix ACK count mismatch (ACKed N but only M pending)
status: Done
assignee:
  - '@claude'
created_date: '2026-06-01 10:04'
updated_date: '2026-07-09 12:21'
labels:
  - protocol
dependencies: []
ordinal: 33000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Surfaced by ably-go TestRealtimeChannel_MessageUpdates, which panics the SDK eventloop with 'protocol violation: ACKed 2 messages, but only 1 pending'. The server emits an ACK whose message count does not match the number of publishes the client has pending for that msgSerial range. An ACK must acknowledge exactly the count of messages the client published; an over-count corrupts the SDK's pending-publish accounting and kills the connection. Audit the ACK/NACK count + msgSerial logic in internal/realtime. Independent of whether message-updates/annotations are in scope.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 An ACK's count equals the number of messages the client published in the acknowledged msgSerial range
- [ ] #2 TestRealtimeChannel_MessageUpdates no longer panics the SDK eventloop on ACK accounting
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Audit the ACK/NACK count + msgSerial accounting in internal/realtime and add a regression test for the create-then-update sequence. Findings: all four ACK/NACK emit sites (handleMessage publish, handleMutation, handlePresence, nack) hardcode Count=1 — TASK-59 fixed the publish+presence over-count and the mutation path (TASK-52/56) was authored as Count=1 from the start; the per-message serials ride the single Res entry. The read loop is single-goroutine and ACKs each frame before reading the next, so ACKs are emitted in msgSerial order with no skip/reorder that could inflate ably-go's serialShift. No remaining over-count path exists, so no production code change is required. Add ack_test.go: (a) create then update over one connection asserts each frame draws exactly one ACK with Count=1 and monotonic msgSerials 1,2; (b) a 3-message publish draws one ACK Count=1 with all 3 serials in one Res entry.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Audit outcome: no remaining over-count path. All ACK/NACK sites emit Count=1 (connection.go:236, mutation.go:93, presence.go:80, presence.go:204). TASK-59 fixed the publish+presence over-count; the mutation path was always Count=1. The single-goroutine read loop ACKs each frame in msgSerial order, so ably-go's serialShift (ackMsgSerial - oldestPending) never inflates the effective count. Confirmed empirically by reproducing the create-then-update sequence: each frame gets exactly one ACK, Count=1, msgSerials 1 then 2. No production code change needed; the regression test locks the invariant in. Note: ordering becomes non-trivial once the store moves off the read goroutine (TASK-20) — this test guards that too.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Fix/guard the ACK-count accounting (regression test). Audited every ACK/NACK emit site in internal/realtime: all four hardcode Count=1, with per-message serials carried in the single Res entry (TR4s). TASK-59 had already eliminated the len(messages)-based over-count on the publish and presence paths; the mutation path was authored as Count=1. Because the read loop processes and ACKs frames serially, ACKs are emitted in msgSerial order, so ably-go's pending-emitter serialShift cannot inflate the effective count. There was no remaining over-count path, so no production code change was required. Added ack_test.go with two regressions: (1) create-then-update over one connection asserts each inbound protocol message draws exactly one ACK with Count=1 and monotonic msgSerials; (2) a 3-message publish draws one ACK Count=1 with all three serials in one Res entry. AC #2 (ably-go TestRealtimeChannel_MessageUpdates no longer panics) is left unchecked: the ably-go integration suite cannot be run in this repo — the unit regression reproduces the same server-side accounting the SDK relies on.
<!-- SECTION:FINAL_SUMMARY:END -->
