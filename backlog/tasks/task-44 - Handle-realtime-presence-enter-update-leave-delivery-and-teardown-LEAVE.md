---
id: TASK-44
title: 'Handle realtime presence: enter/update/leave, delivery, and teardown LEAVE'
status: Done
assignee:
  - '@lmars'
created_date: '2026-06-13 09:37'
updated_date: '2026-06-14 18:32'
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
- [x] #1 Inbound PRESENCE with ENTER/UPDATE/LEAVE is persisted via StorePresence and ACKed on its msgSerial; a failed presence op is NACKed
- [x] #2 Inbound PRESENCE from an attachment without the PRESENCE mode is rejected with NACK
- [x] #3 clientId validation per section 12.3: a mismatched clientId is NACKed; an anonymous connection entering presence is NACKed (code 91000); a wildcard bearer may enter a concrete clientId
- [x] #4 connectionId is server-stamped on every presence message and cannot be set by the client
- [x] #5 Attachments with PRESENCE_SUBSCRIBE receive live PRESENCE frames; attachments without it receive none
- [x] #6 On connection close or DETACH the server emits LEAVE for exactly the members that connection entered on each channel, observed by other subscribers
- [x] #7 Graceful shutdown (section 11) emits the same LEAVEs before the connection is closed
- [x] #8 Tests over the WS protocol cover enter, update, leave, cross-connection delivery, clientId rejection, and implicit-leave on disconnect
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Realtime presence: enter/update/leave, delivery, and teardown LEAVE (DESIGN.md §12.2, §12.3, §12.5).

Inbound PRESENCE (connection.handlePresence): requires an attachment holding the PRESENCE mode (else NACK 40160); resolves+validates each member's clientId per §12.3, stamps connectionId, publishes via core.Channel.PublishPresence (new, delegates to StorePresence), and ACK/NACKs on msgSerial.

clientId: connection now carries a clientID resolved from the ?clientId= query param (the Basic-auth slice of §3.2; the JWT-claim/wildcard path and full table remain TASK-11/TASK-9). resolvePresenceClientID implements the §12.3 rules — anonymous can't enter (NACK 91000), concrete must match or be omitted, wildcard ("*") must supply a concrete id. The wildcard branch is unit-tested directly (not yet reachable over the wire until JWT lands).

Modes: attachments now resolve channel-mode flags from ATTACH.flags (empty = full set, §4.2) and gate frame flow — a kind-aware forward() emits MESSAGE under SUBSCRIBE and PRESENCE under PRESENCE_SUBSCRIBE. Behaviour-preserving for existing flag=0 attaches. Capability intersection (effective = requested ∩ permitted) remains TASK-12.

Teardown LEAVE: the connection tracks the (channel, clientId) members it entered; DETACH leaves that channel's members, and connection termination (readLoop exit — client disconnect, network error, or socket closed on shutdown) synthesises LEAVE for all remaining members on a fresh bounded context, so other subscribers see departures. There is no grace period (§4.3). Graceful DISCONNECT framing on SIGTERM is TASK-2; the LEAVE-on-exit path already covers shutdown once sockets close.

Tests (internal/realtime/presence_test.go): cross-connection enter delivery (resolved clientId + stamped connectionId + serial), update/leave ordering, anonymous reject (91000), clientId mismatch reject, presence-mode required, implicit-leave on disconnect, detach-leaves, plus a table-driven unit test of resolvePresenceClientID incl. the wildcard cases. Full unit suite + integration build green.
<!-- SECTION:FINAL_SUMMARY:END -->
