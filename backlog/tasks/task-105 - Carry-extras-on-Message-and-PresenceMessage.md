---
id: TASK-105
title: Carry extras on Message and PresenceMessage
status: Done
assignee:
  - '@claude'
created_date: '2026-07-12 13:59'
updated_date: '2026-07-12 17:20'
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
- [x] #1 Message extras round-trip realtime publish -> subscriber and publish -> history
- [x] #2 PresenceMessage extras round-trip enter -> presence event
- [x] #3 extras_field and presenceMessageExtras pass
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add Extras (map[string]any) to protocol.Message, PresenceMessage, Annotation (json+msgpack tags). 2. Extend the three custom msgpack encoders in msgpackcodec.go with an 'extras' field; populate Extras in the parity-test full builders. 3. Shallow-mixin in storage.MergeVersion: carry current extras forward, a supplied extras replaces; the append delta carries the merged extras. 4. Unit tests (protocol round-trip JSON+msgpack+storage; storagetest contract across memory/bbolt/postgres; mutation carry-forward). 5. Harness verify against ably-js.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Per Lewis (2026-07-12): AIT depends on setting extras.ai on messages, so this is a hard blocker for the AIT suite run (TASK-96) and for AIT demos — land this BEFORE TASK-96. Extras must round-trip verbatim on publish, fan-out, storage, history, and REST reads, for Message, PresenceMessage and (check) Annotation.

Representation: Extras is map[string]any (not json.RawMessage). The reference (ablyrpc) holds extras as a protobuf structpb.Struct but wire-encodes it as a plain object under BOTH json (MarshalJSON) and msgpack (AsMap -> map[string]any). We have no protobuf, so a plain Go map is the faithful mirror and round-trips both encodings natively; a raw-JSON carrier would not survive the msgpack storage payload (DESIGN.md §6). Added to the custom msgpack encoders (msgpackcodec.go) so the reflection parity test stays green; extras persists verbatim because all three backends msgpack-marshal the whole Message/PresenceMessage/Annotation. Mutation carry-forward mirrors coordinator.buildUpdateMessage: MergeVersion carries the current extras forward (via the struct copy) and a supplied mut.Extras replaces the whole object (shallow-mixin, §13.2), for update/append/delete alike; the append delta carries the merged extras.

Also fixed (required for presenceMessageExtras to pass): StorePresence stamped Serial but not Timestamp, so presence frames carried no timestamp. ably-js decides leave-vs-enter newness by timestamp when a presence message has no id (RTP2b1); undefined>=undefined is false, so the leave was never applied and the test hung after a green enter step. Added storage.StampPresenceMember (Serial + Timestamp from the channelSerial) used by all three backends.

Harness (ably-js worktree, ABLY_ENDPOINT=localhost ABLY_PORT=9080 ABLY_USE_TLS=false, --timeout 15000):
- message.test.js --grep extras_field: PASS (1/1).
- presence.test.js --grep presenceMessageExtras: PASS (1/1).
- message.test.js --grep comet --invert: 41 passing, 1 failing (subscribes to filtered channel — pre-existing filtered/derived-subscription gap, unrelated).
- presence.test.js (full): 28 passing, 3 pending, 6 failing. Baseline (server built from pre-change HEAD) was 22 passing / 12 failing; my change is net +6 with zero regressions (the 6 remaining are a strict subset of baseline: presenceEnterInvalid x2, presence_auto_reenter x2, leave_published_for_member_missing_from_sync, suspended_preserves_presence — all pre-existing non-goals/separate gaps). The timestamp fix also turned green presenceEnterAndLeave, presenceEnterDetachEnter, presenceEnterLeaveGet, presenceEnterUpdate, presence_many_updates.

Gates: go build ./... ; go vet ./... ; go vet -tags=integration ./... ; go test -count=1 ./... ; go test -race ./internal/realtime/ ; go test -tags=integration -race ./internal/storage/postgres/... — all PASS.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Added an Extras field (raw JSON object, preserved verbatim) to protocol.Message, PresenceMessage and Annotation, carried as map[string]any so it round-trips both the JSON and msgpack wire codecs and the msgpack storage payload across all three backends (memory/bbolt/postgres). Extended the custom msgpack encoders (msgpackcodec.go) and the parity-test builders. Extras rides publish, fan-out, storage, history/version reads and REST reads unchanged; mutations apply the §13.2 shallow-mixin (carry forward unless supplied, whole-field replace), including on the append delta. Documented in DESIGN.md §8. While making the required presenceMessageExtras test pass, also fixed a presence wire gap the test newly exposed: presence frames now carry a server Timestamp (storage.StampPresenceMember), without which SDKs (RTP2b1) never apply a leave. ably-js message.test.js extras_field and presence.test.js presenceMessageExtras both pass; full-suite runs show net improvement and no regressions (presence went 22->28 passing vs a pre-change baseline). All build/vet/test/race/integration gates green.
<!-- SECTION:FINAL_SUMMARY:END -->
