---
id: TASK-52
title: 'Realtime mutations: inbound MESSAGE update/delete/append and outbound delivery'
status: Done
assignee: []
created_date: '2026-06-13 14:46'
updated_date: '2026-06-14 21:11'
labels:
  - mutable-messages
dependencies:
  - TASK-49
  - TASK-50
  - TASK-51
documentation:
  - DESIGN.md
ordinal: 52000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Per DESIGN.md sections 13.2 and 13.6. On an inbound MESSAGE frame with action update/delete/append and a target serial, authorise (the attachment must hold PUBLISH mode plus the relevant message-* capability per section 13.5), call Store (which validates the target, applies the shallow-mixin merge, mints the version, persists, and updates the projection/versions index), and ACK or NACK on the msgSerial. Subscribers receive the new version as an ordinary outbound MESSAGE frame carrying action plus version plus the unchanged serial, delivered by the normal attachment cursor in stream order. No new ProtocolMessage action is introduced — mutations reuse MESSAGE (15), distinguished by the Message-level action field.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Inbound MESSAGE with action update/delete/append and a target serial is persisted via Store and ACKed on its msgSerial; failures NACK
- [ ] #2 A mutation requires the PUBLISH attachment mode and the relevant message-* capability; otherwise NACK
- [x] #3 Subscribers receive update/delete as outbound MESSAGE frames carrying action, version, and the unchanged serial, in stream order
- [x] #4 A non-existent or aged-out target serial is rejected with NACK
- [x] #5 No new ProtocolMessage action is introduced; mutations reuse MESSAGE (15)
- [x] #6 WS tests cover update, delete, cross-subscriber delivery, and target-not-found
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
AC#2: the PUBLISH attachment-mode gate is implemented and tested; the message-* capability gate is deferred — the capability framework (TASK-12) is unbuilt, so no capability/ownership check is wired (per project direction). It slots into handleMutation once TASK-12 lands.
<!-- SECTION:NOTES:END -->
