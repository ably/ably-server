---
id: TASK-31
title: Honour the echo connection param (echo=false)
status: Done
assignee:
  - '@claude'
created_date: '2026-06-01 10:02'
updated_date: '2026-07-09 12:20'
labels:
  - protocol
dependencies: []
ordinal: 31000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
ably-go integration test TestRealtimeChannel_Subscribe fails: a connection opened with echo=false still receives its own published messages. The server currently fans every MESSAGE out to all attached connections including the publisher (no 'echo' handling exists in internal/). Read the echo query param on the WS upgrade (default true per Ably), store it on the connection, and in the channel fan-out skip delivery back to the originating connection when echo is false.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 A connection with echo=false does not receive messages it published itself
- [x] #2 A connection with echo=true (default) still receives its own published messages
- [x] #3 Other attached connections receive the message regardless of the publisher's echo setting
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Read the 'echo' query param on the WS upgrade in server.go (default true; only a valid bool flips it) and store echo bool on the connection.
2. Stamp m.ConnectionID = c.id on created messages in handleMessage before Publish (correct Ably wire behaviour: delivered messages carry the publisher's connectionId; also the key echo uses). Mutations already carry the creator's connectionId forward via MergeVersion.
3. Pass echo + connID into newAttachment; in attachment.forward(), when echo is false and a message cm's ConnectionID equals this connection's id, skip delivery (return true). Presence is unaffected (separate branch) so presence is always delivered.
4. Tests: echo=false publisher does not receive its own message but a co-attached echo=true peer does; echo=true (default) publisher does receive its own; verify presence still delivered under echo=false.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
echo is keyed on the stamped Message.connectionId; a mutation carries the creator's connectionId forward (MergeVersion), matching Ably's message-origin semantics. Presence is delivered on a separate fan-out branch, so it is never suppressed.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Honour the echo connection param. The WS upgrade now reads echo (default true; only a valid bool flips it) and stores it on the connection. Published messages are stamped with the publisher's connectionId (correct Ably wire behaviour — delivered Message.connectionId — and the key echo uses). The attachment fan-out skips a message cm whose connectionId equals the receiving connection's id when that connection has echo=false; presence deliveries ride a separate branch and are always delivered. Documented in DESIGN.md §2.1. Tests: echo=false publisher is ACKed but does not receive its own message while a peer does; echo=true (default) publisher receives its own; echo=false still delivers the connection's own presence. The ably-go TestRealtimeChannel_Subscribe AC cannot be run in this repo but the unit tests cover its scenario.
<!-- SECTION:FINAL_SUMMARY:END -->
