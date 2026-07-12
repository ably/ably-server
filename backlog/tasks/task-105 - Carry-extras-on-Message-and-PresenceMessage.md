---
id: TASK-105
title: Carry extras on Message and PresenceMessage
status: To Do
assignee: []
created_date: '2026-07-12 13:59'
updated_date: '2026-07-12 16:37'
labels:
  - compat
  - ably-js
dependencies: []
priority: high
ordinal: 105000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Neither protocol.Message nor protocol.PresenceMessage has an Extras field, so client-supplied extras (headers, ephemeral, push metadata...) are silently dropped on publish, fan-out, storage, and history. Failing tests (2026-07-12 run): realtime/message extras_field ('Check extras is present: expected undefined to deeply equal {headers:{some:metadata}}'), realtime/presence presenceMessageExtras ('extras should have headers key=value'). Fix: add Extras (raw JSON object, preserved verbatim) to both wire types, persist it in the storage backends' message/presence records, and include it in realtime delivery, REST reads, and history. Check annotations too — Annotation likely shares the extras concept.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Message extras round-trip realtime publish -> subscriber and publish -> history
- [ ] #2 PresenceMessage extras round-trip enter -> presence event
- [ ] #3 extras_field and presenceMessageExtras pass
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Per Lewis (2026-07-12): AIT depends on setting extras.ai on messages, so this is a hard blocker for the AIT suite run (TASK-96) and for AIT demos — land this BEFORE TASK-96. Extras must round-trip verbatim on publish, fan-out, storage, history, and REST reads, for Message, PresenceMessage and (check) Annotation.
<!-- SECTION:NOTES:END -->
