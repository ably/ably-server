---
id: TASK-44
title: 'Handle realtime presence: enter/update/leave, delivery, and teardown LEAVE'
status: To Do
assignee: []
created_date: '2026-06-13 09:37'
labels:
  - presence
dependencies:
  - TASK-42
  - TASK-43
documentation:
  - DESIGN.md
ordinal: 44000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Handle presence over the WebSocket per DESIGN.md sections 12.2, 12.3 and 12.5. On an inbound PRESENCE frame the connection authorises (the attachment must hold the PRESENCE mode, granted from the presence capability at attach time), validates the clientId per section 12.3 (it must equal the connection resolved clientId; a wildcard bearer must supply a concrete clientId; an anonymous connection cannot enter presence), stamps connectionId, calls channel.StorePresence, and replies ACK or NACK on the msgSerial. Attachments holding PRESENCE_SUBSCRIBE receive presence cms as outbound PRESENCE frames via the same cursor that delivers MESSAGE frames, gated by mode. When a connection ends (explicit LEAVE/DETACH/CLOSE, read error, heartbeat timeout, or graceful shutdown) the server synthesises a LEAVE for every member that connection entered and publishes them through StorePresence so departures persist, fold out of the set, and reach all subscribers. There is no presence grace period, consistent with section 4.3.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Inbound PRESENCE with ENTER/UPDATE/LEAVE is persisted via StorePresence and ACKed on its msgSerial; a failed presence op is NACKed
- [ ] #2 Inbound PRESENCE from an attachment without the PRESENCE mode is rejected with NACK
- [ ] #3 clientId validation per section 12.3: a mismatched clientId is NACKed; an anonymous connection entering presence is NACKed (code 91000); a wildcard bearer may enter a concrete clientId
- [ ] #4 connectionId is server-stamped on every presence message and cannot be set by the client
- [ ] #5 Attachments with PRESENCE_SUBSCRIBE receive live PRESENCE frames; attachments without it receive none
- [ ] #6 On connection close or DETACH the server emits LEAVE for exactly the members that connection entered on each channel, observed by other subscribers
- [ ] #7 Graceful shutdown (section 11) emits the same LEAVEs before the connection is closed
- [ ] #8 Tests over the WS protocol cover enter, update, leave, cross-connection delivery, clientId rejection, and implicit-leave on disconnect
<!-- AC:END -->
