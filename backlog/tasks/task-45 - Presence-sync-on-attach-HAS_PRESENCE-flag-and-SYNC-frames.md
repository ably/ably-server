---
id: TASK-45
title: 'Presence sync on attach: HAS_PRESENCE flag and SYNC frames'
status: To Do
assignee: []
created_date: '2026-06-13 09:37'
labels:
  - presence
dependencies:
  - TASK-42
  - TASK-43
  - TASK-44
documentation:
  - DESIGN.md
ordinal: 45000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Deliver the current presence set to a newly-attaching subscriber per DESIGN.md section 12.4. When a client attaches with PRESENCE_SUBSCRIBE to a channel whose membership set is non-empty, the server sets the HAS_PRESENCE flag on ATTACHED and then streams the set from Members as one or more SYNC frames (each carrying members as PresenceMessages with action PRESENT) before resuming live delivery. The channelSerial field on SYNC doubles as the sync cursor (serial:cursor while pages follow, serial: with an empty cursor part on the final page). The snapshot is taken at-or-after the attach point; consistency with concurrent live events is the client merge by serial, so no extra server coordination is needed. Sync is gated by PRESENCE_SUBSCRIBE.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 ATTACHED carries HAS_PRESENCE (1<<0) iff the channel membership set is non-empty at attach and the attachment holds PRESENCE_SUBSCRIBE
- [ ] #2 After ATTACHED the server emits the current members as SYNC frames with action PRESENT, then enters live delivery
- [ ] #3 The final SYNC frame signals completion with an empty cursor part in channelSerial; any intermediate page carries a non-empty cursor
- [ ] #4 An attachment without PRESENCE_SUBSCRIBE receives neither HAS_PRESENCE nor any SYNC
- [ ] #5 A member that enters or leaves between the snapshot and the live attach point is delivered again on the live cursor (duplicate ENTER idempotent, later LEAVE supersedes), so the client converges to the correct set
- [ ] #6 Empty membership set: no HAS_PRESENCE and no SYNC
- [ ] #7 Tests cover sync of a non-empty set on attach, no-sync for an empty set, no-sync without PRESENCE_SUBSCRIBE, and convergence when a live event races the snapshot
<!-- AC:END -->
