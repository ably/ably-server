---
id: TASK-59
title: 'ACK count must be per protocol-message, not per inner message'
status: Done
assignee: []
created_date: '2026-06-14 22:38'
updated_date: '2026-06-14 22:39'
labels:
  - protocol
  - realtime
dependencies: []
ordinal: 59000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
An ACK acknowledges protocol messages: msgSerial + count specify the range of msgSerials acknowledged (msgSerial .. msgSerial+count-1), one msgSerial per sent protocol message. The server instead sets count to the number of inner Messages/PresenceMessages in the frame.

- Realtime publish ACK: Count = len(messages) (connection.go).
- Presence ACK: Count = len(presence) (presence.go).

ably-go assigns one msgSerial per send() and acks queue[:count] FRAMES, panicking when count > pending (state.go: 'protocol violation: ACKed N messages, but only M pending'). So a PublishMultiple / multi-member presence of N≥2 sends count=N for a single msgSerial and over-acks (panics the SDK). Single-item frames work only because N=1.

The server emits exactly one ACK per inbound frame and never batch-acks, so the correct value is Count=1. The per-message serials already travel in the ACK's Res array (one Res entry per frame, holding that frame's serials), so count and res are independent.

Pre-existing (predates mutable messages); flagged in TASK-58's notes.

Scope:
- Publish ACK Count=1; Res = one entry carrying all the frame's message serials.
- Presence ACK Count=1.
- Replace the test that asserts count==batch-size.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Realtime publish ACK has Count=1 regardless of how many messages the frame carries
- [x] #2 Presence ACK has Count=1 regardless of member count
- [x] #3 A multi-message publish ACK's Res carries all the frame's serials in one entry
- [x] #4 Test asserts per-protocol-message count (replacing the batch-size expectation)
<!-- AC:END -->
