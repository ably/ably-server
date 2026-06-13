---
id: TASK-31
title: Honour the echo connection param (echo=false)
status: To Do
assignee: []
created_date: '2026-06-01 10:02'
updated_date: '2026-06-03 13:06'
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
- [ ] #1 A connection with echo=false does not receive messages it published itself
- [ ] #2 A connection with echo=true (default) still receives its own published messages
- [ ] #3 Other attached connections receive the message regardless of the publisher's echo setting
<!-- AC:END -->
