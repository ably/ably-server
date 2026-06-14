---
id: TASK-45
title: 'Presence sync on attach: HAS_PRESENCE flag and SYNC frames'
status: Done
assignee:
  - '@lmars'
created_date: '2026-06-13 09:37'
updated_date: '2026-06-14 18:38'
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
- [x] #1 ATTACHED carries HAS_PRESENCE (1<<0) iff the channel membership set is non-empty at attach and the attachment holds PRESENCE_SUBSCRIBE
- [x] #2 After ATTACHED the server emits the current members as SYNC frames with action PRESENT, then enters live delivery
- [x] #3 The final SYNC frame signals completion with an empty cursor part in channelSerial; any intermediate page carries a non-empty cursor
- [x] #4 An attachment without PRESENCE_SUBSCRIBE receives neither HAS_PRESENCE nor any SYNC
- [x] #5 A member that enters or leaves between the snapshot and the live attach point is delivered again on the live cursor (duplicate ENTER idempotent, later LEAVE supersedes), so the client converges to the correct set
- [x] #6 Empty membership set: no HAS_PRESENCE and no SYNC
- [x] #7 Tests cover sync of a non-empty set on attach, no-sync for an empty set, no-sync without PRESENCE_SUBSCRIBE, and convergence when a live event races the snapshot
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Presence sync on attach: HAS_PRESENCE flag + SYNC frames (DESIGN.md §12.4).

In attachment.run(), a PRESENCE_SUBSCRIBE attachment now snapshots the channel's membership set (core.Channel.Members → storage.Members) before sending ATTACHED. If the set is non-empty, ATTACHED carries the HAS_PRESENCE flag and the set is then delivered as a single SYNC frame whose Presence members are stamped action=PRESENT (copied, never mutating the stored set), with channelSerial = "<asOf>:" — the empty cursor part marking the set complete. An empty set, or an attachment without PRESENCE_SUBSCRIBE, gets neither HAS_PRESENCE nor a SYNC.

Convergence is automatic: the snapshot is taken at-or-after the live anchor captured at Attach, so any member that enters/leaves past the anchor also arrives on the live cursor; the synced member (whose enter precedes the anchor) is not re-delivered live, so there is no duplicate. SYNC paging for very large sets is deferred (single frame at our scale; AC #3 intermediate-cursor path noted in the Status callout).

Tests (presence_test.go): sync of a non-empty set (HAS_PRESENCE + SYNC with action PRESENT + complete cursor), no-sync/no-flag for an empty channel, no-sync/no-flag without PRESENCE_SUBSCRIBE (proved by the next frame being a MESSAGE), and snapshot-then-live convergence (alice via SYNC, bob via live, no alice duplicate). Full unit suite, vet, and integration build green.
<!-- SECTION:FINAL_SUMMARY:END -->
