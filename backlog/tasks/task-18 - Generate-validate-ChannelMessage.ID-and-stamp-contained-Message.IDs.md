---
id: TASK-18
title: Generate/validate ChannelMessage.ID and stamp contained Message.IDs
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 16:11'
updated_date: '2026-07-09 11:57'
labels:
  - protocol
dependencies:
  - TASK-13
ordinal: 18000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Add an ID field to protocol.ChannelMessage. On publish: if ChannelMessage.ID is unset, generate a random 8-character base64 string and set each contained Message.ID = "<id>:<idx>". If ChannelMessage.ID is set (client-supplied), validate that each contained Message.ID equals the expected "<id>:<idx>", rejecting/NACKing on mismatch. This batch id is the idempotency key indexed by storage (the bbolt `ids` sub-bucket / Postgres unique index on message id, DESIGN §6/§8). Implement in the publish path that is being consolidated under storage. Depends on the serial-generation consolidation task.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Unset ChannelMessage.ID yields an 8-char base64 id with each Message.ID set to "<id>:<idx>"
- [x] #2 A client-supplied ChannelMessage.ID with mismatched contained Message.IDs is rejected
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add ID field to protocol.ChannelMessage (the atomic-publish batch id / idempotency key), json+msgpack 'id,omitempty'.
2. Add id.NewMessageBaseID() -> 8-char base64url (6 random bytes).
3. Add storage.StampMessageIDs(msgs) (batchID, err): mirrors Ably's getMessageBaseID + generation. If no message carries an ID, generate an 8-char base64 batch id and stamp each Message.ID='<id>:<idx>' (idx non-padded, matching Ably 'TojWzTkLiH:0' shape). If client-supplied, derive the base and validate each contained Message.ID equals '<id>:<idx>'; reject with storage.ErrInvalidMessageID on mismatch. Single-message client id is accepted as-is (base = TrimSuffix(id,':0')), matching Ably.
4. Call StampMessageIDs at the top of each backend Store (memory/bbolt/postgres); set cm.ID = batchID on the returned/persisted cm. Idempotency stays keyed on the per-message ids, which now embed the batch id (reconciling with the existing bbolt 'ids' bucket / Postgres partial unique index) so a duplicate batch collides on its first message id.
5. Map storage.ErrInvalidMessageID -> 400 (REST) / NACK (WS, already the default).
6. Update storagetest contract suite: rewrite the shared-id idempotency test to batch semantics, add generation + mismatch-rejection tests (AC#1/#2), run across memory+bbolt.
7. Update DESIGN sec6/sec8.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implemented via a shared storage.StampMessageIDs helper called at the top of every backend Store (memory/bbolt/postgres), so REST and WS both get identical batch-id handling. Added ID field to protocol.ChannelMessage and id.NewMessageBaseID (8-char base64url). Idempotency stays keyed on the per-message ids, which now embed the batch id, so the existing bbolt 'ids' bucket / Postgres partial unique index are reused unchanged; a duplicate client-idempotent batch collides on its first message id. Validation of client-supplied ids mirrors Ably's getMessageBaseID (single-message: any id, base = id trimmed of a trailing ':0'; multi-message: each id must be '<base>:<idx>'). Mismatch -> storage.ErrInvalidMessageID (REST 400, WS NACK). Contract suite updated: replaced the old per-message shared-id idempotency test with batch-id generation, mismatch-rejection and batch-idempotency subtests (run across memory+bbolt); added a direct StampMessageIDs unit test.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Add a batch id to protocol.ChannelMessage and stamp/validate contained Message.IDs on the consolidated publish path. New storage.StampMessageIDs helper, called by every backend's Store, generates a random 8-char base64 batch id when the publisher supplies no message ids and stamps each Message.ID='<batchID>:<idx>' (unpadded idx, matching Ably's wire shape); when ids are client-supplied it derives the batch id and validates conformance, rejecting mismatches with storage.ErrInvalidMessageID (mapped to REST 400 / WS NACK). ChannelMessage.ID is set on the persisted/returned cm. Idempotency continues to key on per-message ids (now batch-derived), reconciling with the existing bbolt ids bucket and Postgres partial unique index. DESIGN §8 updated. Unit coverage: storage helper test + storagetest contract subtests (generation, mismatch rejection, batch idempotency) across memory and bbolt. Postgres backend shares the same code path but its contract tests require a live DB and were not run here.
<!-- SECTION:FINAL_SUMMARY:END -->
