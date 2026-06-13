---
id: TASK-43
title: 'Add presence to the storage boundary: stream kind, StorePresence, Members'
status: Done
assignee:
  - '@lmars'
created_date: '2026-06-13 09:37'
updated_date: '2026-06-13 15:20'
labels:
  - presence
dependencies:
  - TASK-42
documentation:
  - DESIGN.md
ordinal: 43000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Extend the storage boundary to persist presence and materialise the membership set, per DESIGN.md sections 6, 6.3 and 12.5. Messages and presence share one ordered append-only log and one channelSerial namespace, distinguished by a kind discriminator (the string message or presence) — so the single message log table/bucket is renamed to channel_messages to reflect that it carries both, and the membership set lives in a separate presence projection table (mirroring how mutable messages will later add a messages projection over the same log; DESIGN.md section 6.3). Add ChannelStore.StorePresence(ctx, presence) which mints a channelSerial, stamps each PresenceMessage.Serial, persists the presence cm onto the log, and in the same atomic step folds it into the membership set (ENTER/UPDATE upsert the member keyed by connectionId:clientId, LEAVE removes it), then delivers the cm via the Appender exactly like a message publish. Add ChannelStore.Members(ctx) returning the current set plus the channelSerial it is current as-of. History gains a kind selector so message history and presence history each scan only their own cms. Implement across memory, bbolt and postgres behind the shared contract suite. The membership set is process-lifetime in memory and bbolt (not persisted; presence is connection-scoped) and the presence table in postgres.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 HistoryQuery carries a kind selector; message-history scans exclude presence cms and presence-history scans exclude message cms, on all three backends
- [x] #2 StorePresence mints a channelSerial, stamps each PresenceMessage.Serial to channelSerial:idx, persists the presence cm, and delivers it via the Appender (synchronous for memory/bbolt, via LISTEN/NOTIFY for postgres)
- [x] #3 StorePresence folds the operation into the membership set atomically with the persist: ENTER and UPDATE upsert a member keyed by connectionId:clientId; LEAVE removes it
- [x] #4 A client-supplied PresenceMessage.ID is honoured for idempotency on the same per-channel index as message IDs
- [x] #5 Members returns the current membership set and the channelSerial the set is current as-of; an empty set returns cleanly
- [x] #6 memory and bbolt hold the membership set in memory only (empty after a fresh Open); presence history still persists as ordinary stream cms
- [x] #7 The shared storage contract suite covers StorePresence, Members, idempotent presence, kind-filtered history, and LEAVE removing a member, and passes on memory, bbolt and postgres
- [x] #8 The message log table/bucket is renamed messages -> channel_messages (forward-only postgres migration plus bbolt bucket rename); existing message history reads it unchanged after the rename
- [x] #9 postgres adds the kind column on channel_messages and creates the presence projection table; auto-migrate applies the migration on startup
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Greenfield: edit migrations directly, no compat layer. Scope = presence only (materialised messages projection + message_serial + versions index are TASK-50).
1. storage.go: Kind type (message|presence, empty==message); HistoryQuery.Kind; ChannelStore.StorePresence(ctx, []*PresenceMessage) (cm, idempotent, err) and Members(ctx) ([]*PresenceMessage, asOfSerial, err).
2. core.Channel.Append: stop dropping cms that carry Presence (current guard requires Messages).
3. memory: one stream holds both kinds; History filters by Kind; StorePresence mints+stamps+stores+folds membership map+fires appender; Members reads the map.
4. bbolt: rename messages bucket->channel_messages; presence cms persist with kind in the blob; membership held in-memory (empty after Open); History decodes+filters by kind.
5. postgres: edit 0001 (rename messages->channel_messages, add kind); add 0004_presence.sql (presence projection table); Store sets kind=message; StorePresence inserts kind=presence + upserts/deletes presence table + NOTIFY; Members SELECTs presence; LISTEN decodes per-kind.
6. storagetest: shared presence contract tests (StorePresence+Members, LEAVE removal, idempotent presence, kind-filtered history). bbolt_test: membership empty after restart, presence history persists.
<!-- SECTION:PLAN:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Extended the storage boundary for presence and renamed the log table (DESIGN.md §6, §6.3, §12).

Interface (storage.go): ChannelStore gains StorePresence (mint + stamp Serial + persist on the shared stream as kind=presence + fold the membership set, atomically) and Members (current set + as-of watermark). HistoryQuery gains Kind; the shared CMItems helper drives kind-aware iteration so message and presence history reuse identical pagination/limit/cursor logic.

Rename (greenfield, edited in place): the message log messages -> channel_messages — Postgres migration 0001 rewritten + bbolt bucket renamed. Message history reads unchanged.

Membership projection: Postgres presence table (new migration 0004), authoritative across nodes, upserted/deleted in the publish tx; in-memory map for memory and bbolt (process-lifetime, empty on Open — presence history still persists). core.Channel.Append no longer drops presence cms. Postgres Store/StorePresence stamp kind, the LISTEN loader decodes per-kind, idempotency index shared across kinds.

The materialised messages projection + message_serial + versions index are out of scope here (mutable messages, TASK-50).

Tests: presence contract tests added to storagetest (enter/members, update-replaces, leave-removes, same-client-distinct-conns, idempotent-by-id, kind-filtered history) pass on all three backends; new bbolt restart test proves membership is not persisted while presence history is; Postgres integration + cmd cluster integration green.
<!-- SECTION:FINAL_SUMMARY:END -->
