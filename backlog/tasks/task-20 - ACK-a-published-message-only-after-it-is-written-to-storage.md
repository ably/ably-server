---
id: TASK-20
title: ACK a published message only after it is written to storage
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 16:11'
updated_date: '2026-07-09 12:32'
labels:
  - protocol
dependencies:
  - TASK-13
ordinal: 20000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Only return ACK to the publisher once storage.AppendChannelMessage has durably persisted the ChannelMessage (NACK on failure), rather than acking optimistically. The wiring must not block the connection's read/loop goroutine while the storage write is in flight (DESIGN §5.2 — single writer, non-blocking loop): perform the append off the connection loop and deliver ACK/NACK (echoing the publish msgSerial) via the outbound writer when it completes, preserving per-connection ordering of acknowledgements. Depends on the serial-generation consolidation (which routes minting + append through storage).
<!-- SECTION:DESCRIPTION:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Move the storage write off the read goroutine while preserving per-connection ACK/NACK ordering.
1. Add a per-connection FIFO publish worker: a buffered chan func() (publishQ) drained by one publishLoop goroutine started in run(). One task per inbound MESSAGE/mutation/PRESENCE frame; the worker runs them in arrival order (== msgSerial order), so ACK/NACK are emitted in order and a single worker also keeps per-connection channel-append order (concurrent per-connection stores would reorder channelSerials — undesirable).
2. Read loop keeps all in-memory validation and map access (clientId resolve + connectionId stamp, presence attachment/mode checks, entered-set updates done optimistically, mutation detection) and enqueues either a store task or a NACK-only task — so even validation NACKs stay ordered behind prior pending publishes.
3. The task performs GetChannel + Publish / Mutate / PublishPresence and emits exactly one ACK (after durable commit) or NACK (on failure) via c.queue. GetChannel moves into the message/mutation task so read-loop stays non-blocking.
4. run(): start publishLoop; on read-loop exit cancel ctx, wait for publishLoop, then emitTeardownLeaves (still synthesises presence LEAVEs — worker never touches the entered map, so no race), then wait writeDone.
5. Tests: ACK follows durable commit (readable via history) and is NACKed on store failure (inject a failing store); ordering preserved when an early store is slow (ACKs still msgSerial-ordered). Update DESIGN.md §5.2 to describe the off-loop publish worker. Note existing Publish-returns-after-commit already holds.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Publish-returns-after-commit already held (Channel.Publish delegates to storage.Store which commits before returning); the new work is the off-loop, order-preserving delivery. A single FIFO worker per connection is used deliberately — concurrent per-connection stores would reorder channelSerials and ACKs. Presence membership (entered set) is updated optimistically on the read goroutine (its only writer), so a rare store failure yields at worst a harmless synthesised LEAVE. emitTeardownLeaves runs after the worker is drained, so teardown LEAVEs are unaffected (DESIGN §12.5) and race-free.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
ACK a publish only after durable commit, off the read goroutine, preserving per-connection ACK ordering. Added a per-connection FIFO publish worker (publishQ + publishLoop): the read goroutine validates/stamps the frame and enqueues one task per MESSAGE/mutation/PRESENCE (including validation-rejection NACKs, so they stay ordered), while the worker performs GetChannel + Publish/Mutate/PublishPresence and emits the ACK (post-commit) or NACK (on failure) via the outbound writer. A single worker keeps this connection's channel appends in publish order and its ACK/NACK in msgSerial order; the read loop no longer blocks on the storage write. run() starts the worker, drains it on read-loop exit before synthesising teardown LEAVEs (still emitted; the worker never touches the entered set, so no race). Updated DESIGN.md §5.2. Tests (all under -race): non-blocking (an ATTACH is answered while a publish store is gated) with ACK only after the gate releases; NACK on store failure; ACK ordering preserved across two gated publishes. No ACs were defined on this task.
<!-- SECTION:FINAL_SUMMARY:END -->
