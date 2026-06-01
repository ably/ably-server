---
id: TASK-16
title: 'Always expose a channelSerial for a stream, even when the channel is empty'
status: Done
assignee:
  - '@lmars'
created_date: '2026-05-31 16:11'
updated_date: '2026-06-01 18:17'
labels: []
dependencies:
  - TASK-13
ordinal: 16000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A stream/attachment should always have a channelSerial to return in ATTACHED — even for an empty channel — so the client always gets a resumable attach point. Today core.Stream.ChannelSerial() returns "" when parked at the sentinel (no ChannelMessage delivered yet). Since serial minting is being consolidated into storage, the empty-channel attach point should come from storage (the channel's current head/cursor serial), so a fresh attach to an empty channel still yields a non-empty channelSerial the client can later resume from. Depends on serial generation living in storage.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 go test -tags=integration -count=1 ./... is green
- [ ] #2 Smoke-tested via the binary: empty-channel ATTACH returns a non-empty channelSerial in ATTACHED
<!-- DOD:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 storage.Appender gains Initialize(channelSerial string); storage.Storage.Channel becomes Channel(ctx, name, appender) (ChannelStore, error) and synchronously invokes appender.Initialize with the channel's watermark before returning
- [x] #2 Postgres backend mints serials inside the publish transaction via a new advance_channel_serial SQL function backed by a channels(name, channel_serial) table — total cluster ordering, replacing the per-process serial.Generator and the per-channel advisory lock
- [x] #3 Postgres watermark for a freshly-materialised channel comes from a new ensure_channel SQL function (INSERT ON CONFLICT) so two nodes racing to materialise an empty channel converge on a single watermark
- [x] #4 Memory and bbolt backends call appender.Initialize with a fresh gen.Mint() at first Channel(...) call; their existing single-process generator remains monotonic per-process
- [x] #5 core.Channel is created in a not-ready state; Initialize(serial) seeds the sentinel's serial-only ChannelMessage and closes ready; Attach(ctx) blocks on ready and respects ctx cancellation
- [x] #6 Stream.ChannelSerial() returns the cursor's cm.ChannelSerial unconditionally (cursor.cm is never nil on a ready Channel) — non-empty for any successful Attach
- [x] #7 Manager.GetChannel becomes GetChannel(ctx, name) (*Channel, error); realtime/connection threads ctx through to Channel.Attach and to subsequent Publish calls
- [x] #8 Existing storagetest contract suite continues to pass against all three backends; new contract subtests cover (a) Channel calls appender.Initialize with a non-empty serial, (b) a second Channel(name, ...) call does not re-Initialize the original appender
- [x] #9 Postgres-specific integration test asserts strictly monotonic channel_serials in messages rows after concurrent publishes from two postgres.Storage instances against the same DB
- [x] #10 Existing 'fresh ATTACH returns empty channelSerial' expectations across realtime and core tests are flipped to 'non-empty channelSerial' — verified empirically end-to-end against the binary
- [x] #11 Migration 0002_channels_and_serial_mint.sql adds the channels table, format_channel_serial / next_channel_serial / ensure_channel / advance_channel_serial SQL functions; no changes to the messages table
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## What we need

A non-empty channelSerial for every ATTACHED, so a client that attaches before any publish can still resume from a meaningful cursor.

## Where the serial comes from

Storage owns the serial.Generator (TASK-13). For a non-empty channel, the channel's latest persisted cm.ChannelSerial is the natural watermark — any future publish will sort strictly after it. For an empty channel, a fresh gen.Mint() produces a serial that is strictly greater than every previous mint and strictly less than every future one in this process.

Cluster note: in postgres mode, two empty-channel attachers on different nodes can mint watermarks that interleave with each other and with the first publish, distinguished only by seriesId. This is acceptable because the SDK overwrites its cursor on every inbound MESSAGE; the watermark only matters as the initial cursor when no MESSAGE has been delivered yet. The race that matters — 'client A sees a publish then disconnects with the publish's serial as cursor' — is unaffected.

## 1. Storage

Add to storage.ChannelStore:

  Watermark(ctx context.Context) (string, error)

Implementations:
- memory: under cs.mu — if len(cs.order) > 0 return cs.order[len-1]; else return cs.gen.Mint()
- bbolt: View — cursor.Seek(nextPrefix(prefix)); if returned key is nil, cursor.Last(); then Prev() and check prefix; decode key to extract channelSerial; else return cs.gen.Mint()
- postgres: SELECT channel_serial FROM messages WHERE channel = $1 ORDER BY channel_serial DESC LIMIT 1; if no rows, return cs.gen.Mint()

The generator is already plumbed into each channelStore via TASK-13; no new wiring needed.

## 2. core

Channel.Attach signature: Attach(ctx context.Context) (*Stream, error).

Implementation under c.mu:
- capture cur := c.tail
- compute watermark:
  - if cur.cm != nil: watermark = cur.cm.ChannelSerial
  - else: watermark, err = c.store.Watermark(ctx); release mu while resolving the store-side mint to avoid holding c.mu through I/O
- return &Stream{cursor: cur, watermark: watermark}

Stream struct gains a watermark field. Stream.ChannelSerial() returns cursor.cm.ChannelSerial if non-nil cm, else watermark.

The lock-release subtlety: we want the linkage between 'captured tail' and 'watermark fetched' to be atomic for cluster correctness. Easiest is to compute watermark FIRST (calling store.Watermark) and THEN capture the tail under mu — that way a publish that lands between the two will be in the linked list past the tail, and the watermark from storage is <= that publish's serial. Stream.ChannelSerial() will reflect the live cm once Next() returns it. Concrete order: ctx-bound storage call first, then locked tail capture.

## 3. realtime

connection.handleAttach: pass ctx through to ch.Attach(ctx); propagate error by emitting an ERROR frame (code TBD — pick an existing Ably code for attach failures or invent one and align with TASK-12).

## 4. Tests

- storagetest: WatermarkEmptyChannelIsMintedAndMonotonic (call Watermark twice, expect non-empty + strictly increasing); WatermarkOnPopulatedChannelMatchesLatest (publish N, watermark == latest channelSerial); WatermarkAdvancesAfterPublish (watermark1, publish, watermark2 > watermark1)
- core/channel_test.go: rewrite the two existing 'ChannelSerial() == ""' assertions to assert non-empty + monotonic across two successive Attach(ctx) calls; pass ctx through where needed
- realtime: an SDK test that ATTACHes an empty channel and asserts ATTACHED.ChannelSerial is non-empty
- REST helper updates: server_test.go uses manager.GetChannel(...).Attach() in three places — update to .Attach(ctx)

## Out of scope

- The full resume flow (TASK-14)
- Retention-based aging of watermarks (TASK-26)
- ATTACH error response shape if Attach fails (minimal pass-through for now)
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Plan refinement: dropped the History-based gap-fill from the cluster-race handling. Buffer-and-dedup against the post-init watermark is sufficient because storage is the source of truth — any cm with serial <= X that is absent from this node's live linked list is still in storage and reachable via History on resume.

Plan expanded to incorporate DB-minted serials for cluster total ordering. Adds: channels table tracking the latest channel_serial per channel; advance_channel_serial + ensure_channel SQL functions; postgres backend mints inside the publish txn via the function (drops the per-channel advisory lock — channels-row UPDATE provides serialisation). Memory/bbolt keep the in-process Generator; all three backends expose Watermark/Initialize so the appender always receives an initial serial before any Append. Eliminates the buffer-and-dedup machinery: with DB-minted serials, every post-init Append is guaranteed > the synthetic's serial.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
## Always-non-empty channelSerial + DB-minted serials (TASK-16)

Implements TASK-16's stated goal — every ATTACHED carries a non-empty channelSerial, even on a brand-new attach to a never-published channel — and folds in a deeper architectural improvement: postgres-mode channelSerials are now minted by the database via a new channels table, giving total cluster ordering and eliminating the previous seriesId-tiebreaker race.

## What changed

- **Channel lifecycle**. core.Channel is created in a not-ready state and exposes a new Initialize(serial) method (implements storage.Appender). The storage backend calls Initialize synchronously inside Storage.Channel with the channel's initial watermark; the sentinel head's cm is seeded with that serial. Attach(ctx) blocks on ready and threads context cancellation. Stream.ChannelSerial drops its nil-cm fallback — the cursor's cm is never nil for a ready Channel.

- **Storage interface**. Appender gains Initialize(channelSerial). Storage.Channel becomes Channel(ctx, name, appender) (ChannelStore, error) and synchronously calls appender.Initialize before returning.

- **Postgres: DB-minted serials**. New migration 0002 adds a channels(name, channel_serial) table plus four SQL functions: format_channel_serial, next_channel_serial, ensure_channel, advance_channel_serial. Store advances the channels row inside the publish transaction (the row lock serialises writers per channel — the previous pg_advisory_xact_lock is gone). Storage.Channel materialises via ensure_channel. The per-process serial.Generator is replaced for postgres mode; series remains in the serial format as an informational per-process tag.

- **Memory / bbolt unchanged at the storage layer**. Both still mint locally via the in-process Generator (single-process, no race), but now call appender.Initialize at first Channel call.

- **Manager + realtime**. Manager.GetChannel becomes GetChannel(ctx, name) (*Channel, error). connection.handleAttach and handleMessage thread context through and handle the new error path. REST publish/history handlers do the same.

## Cluster property gained

The previous design had a real edge case where two postgres nodes could mint serials at the same ms that lex-ordered out of commit order (disambiguated only by seriesId). The new channels-row mint produces a single total order regardless of publishing node. A new postgres integration test exercises this: 200 concurrent publishes from two postgres.Storage instances against the same DB produce strictly monotonic channel_serials in the messages rows.

## Tests

- All three backends still pass the (now larger) shared contract suite.
- Postgres testcontainer suite green, including the new ClusterSerialsAreStrictlyMonotonic test.
- realtime: existing ChannelSerial='' expectation flipped to 'non-empty watermark'.
- Smoke-tested end-to-end: WS ATTACH to a never-published channel returns ATTACHED with channelSerial = '01780337290501-000@698d7c5e51'.

## Out of scope (carries forward)

- Resume via history then live tail (TASK-14, now unblocked).
- Capability enforcement on attach (TASK-12).
- Retention / TTL on channels rows (TASK-26).
- The narrow buffer-and-dedup window inside Storage.Channel's initialize path: now fully closed in postgres mode (advance_channel_serial guarantees post-init Appends have serial > the initial watermark).
<!-- SECTION:FINAL_SUMMARY:END -->
