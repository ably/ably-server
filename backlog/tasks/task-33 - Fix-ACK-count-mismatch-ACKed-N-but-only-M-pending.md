---
id: TASK-33
title: Fix ACK count mismatch (ACKed N but only M pending)
status: To Do
assignee: []
created_date: '2026-06-01 10:04'
labels: []
dependencies: []
ordinal: 33000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Surfaced by ably-go TestRealtimeChannel_MessageUpdates, which panics the SDK eventloop with 'protocol violation: ACKed 2 messages, but only 1 pending'. The server emits an ACK whose message count does not match the number of publishes the client has pending for that msgSerial range. An ACK must acknowledge exactly the count of messages the client published; an over-count corrupts the SDK's pending-publish accounting and kills the connection. Audit the ACK/NACK count + msgSerial logic in internal/realtime. Independent of whether message-updates/annotations are in scope.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 An ACK's count equals the number of messages the client published in the acknowledged msgSerial range
- [ ] #2 TestRealtimeChannel_MessageUpdates no longer panics the SDK eventloop on ACK accounting
<!-- AC:END -->
