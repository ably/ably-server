---
id: TASK-43
title: 'Add presence to the storage boundary: stream kind, StorePresence, Members'
status: To Do
assignee: []
created_date: '2026-06-13 09:37'
updated_date: '2026-06-13 14:45'
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
- [ ] #1 HistoryQuery carries a kind selector; message-history scans exclude presence cms and presence-history scans exclude message cms, on all three backends
- [ ] #2 StorePresence mints a channelSerial, stamps each PresenceMessage.Serial to channelSerial:idx, persists the presence cm, and delivers it via the Appender (synchronous for memory/bbolt, via LISTEN/NOTIFY for postgres)
- [ ] #3 StorePresence folds the operation into the membership set atomically with the persist: ENTER and UPDATE upsert a member keyed by connectionId:clientId; LEAVE removes it
- [ ] #4 A client-supplied PresenceMessage.ID is honoured for idempotency on the same per-channel index as message IDs
- [ ] #5 Members returns the current membership set and the channelSerial the set is current as-of; an empty set returns cleanly
- [ ] #6 memory and bbolt hold the membership set in memory only (empty after a fresh Open); presence history still persists as ordinary stream cms
- [ ] #7 The shared storage contract suite covers StorePresence, Members, idempotent presence, kind-filtered history, and LEAVE removing a member, and passes on memory, bbolt and postgres
- [ ] #8 The message log table/bucket is renamed messages -> channel_messages (forward-only postgres migration plus bbolt bucket rename); existing message history reads it unchanged after the rename
- [ ] #9 postgres adds the kind column on channel_messages and creates the presence projection table; auto-migrate applies the migration on startup
<!-- AC:END -->
