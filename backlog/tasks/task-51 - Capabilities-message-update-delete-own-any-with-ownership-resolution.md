---
id: TASK-51
title: 'Capabilities: message-update/delete-own/any with ownership resolution'
status: Done
assignee:
  - '@claude'
created_date: '2026-06-13 14:46'
updated_date: '2026-07-09 13:19'
labels:
  - auth
dependencies:
  - TASK-50
  - TASK-12
documentation:
  - DESIGN.md
ordinal: 51000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Per DESIGN.md sections 3.1 and 13.5. Add the four capability ops message-update-own, message-update-any, message-delete-own, message-delete-any (append is gated by message-update-*). Extend capability resolution with an ownership dimension, the first ownership-scoped op in the model: -own resolves only if the caller's resolved clientId (section 3.2) equals the target message's creator clientId; -any waives the check. The creator clientId comes from the message's current version in storage (TASK-50). Wire the check into both the realtime and REST mutation paths. Creating a message stays plain publish; all version reads stay history.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 The four message-{update,delete}-{own,any} ops parse from x-ably-capability and from the API key capability
- [x] #2 An update/append authorises against message-update-own/any; a delete against message-delete-own/any
- [x] #3 -own permits the op only when the caller's resolved clientId equals the target message's creator clientId; -any skips the check
- [x] #4 Insufficient capability is rejected (WS NACK or ERROR; REST 401)
- [x] #5 Creating a message remains plain publish; version reads remain history
- [x] #6 Tests cover own-allowed, own-denied (different clientId), any-allowed, and missing-capability
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. auth: add Capability.MutationGrant(channel, ownOp, anyOp) -> outcome {Allowed, NeedsOwnership, DeniedCapability}; tests.
2. realtime/mutation.go handleMutation: after GetChannel on the worker, resolve required op pair from action (update/append->message-update-*, delete->message-delete-*); MutationGrant; if NeedsOwnership look up target's creator via ch.LatestVersion and require caller c.clientID (concrete) == creator; deny -> NACK 40160 (target-not-found -> 40400/404). Then Mutate.
3. rest HandleMutate: resolve request clientId; same authorization before ch.Mutate; deny -> 401 Ably error (40160); stamp operator clientId consistently.
4. Update mutation.go / HandleMutate doc comments (drop TASK-12/TASK-51 deferral notes). Creating stays plain publish; version reads stay history (unchanged).
5. Tests: own-allowed, own-denied (different clientId), any-allowed, missing-capability — on both WS and REST.
<!-- SECTION:PLAN:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Ownership-scoped mutation ops (TASK-51). The four message-{update,delete}-{own,any} ops parse from x-ably-capability and the key capability (TASK-12). A new Capability.MutationGrant resolves a mutation to Allowed (-any granted, ownership waived), NeedsOwnership (only -own granted), or DeniedCapability. Both the realtime (handleMutation) and REST (HandleMutate) mutation paths authorise before Mutate: update/append gate on message-update-*, delete on message-delete-*; when only -own is held the target's creator clientId is read from its current version (LatestVersion) and the mutation proceeds only if the caller's resolved concrete clientId equals it. Insufficient capability is a WS NACK 40160 / REST 401 (Ably error shape); a missing target under an ownership check is 40400/404. Creating a message stays plain publish and version reads stay history. Tests cover own-allowed, own-denied (different clientId), any-allowed, and missing-capability on both WS and REST, plus the update-vs-delete op split.
<!-- SECTION:FINAL_SUMMARY:END -->
