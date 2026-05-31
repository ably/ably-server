---
id: TASK-13
title: Consolidate serial generation into the storage backend
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 16:11'
updated_date: '2026-05-31 17:23'
labels: []
dependencies: []
ordinal: 13000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Serial minting is currently duplicated: core.Channel.Append mints a channelSerial via its own serial.Generator and stamps each Message.Serial, while internal/storage/storage.go documents that the backend owns minting and persists monotonic state (DESIGN §6/§8). Make storage the sole authority: AppendChannelMessage mints the channelSerial, stamps each Message.Serial = "<channelSerial>:<idx>", persists, and returns the resulting ChannelMessage. Remove the gen field and the minting from core.Channel; change the publish path so Channel.Append takes the already-minted ChannelMessage (channel.Append(cm)) rather than raw messages. This lets persistent backends restore generator monotonicity across restarts (the bbolt _meta bucket). Update the DESIGN §5.1 Channel sketch to match. Foundational for "always expose a channelSerial" and "ACK only after storage write".
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 core.Channel no longer holds a *serial.Generator (the gen field is removed); it holds a storage.ChannelStore reference instead.
- [x] #2 core.Channel.Append's signature becomes Append(cm *protocol.ChannelMessage) — it links the already-minted cm into the live list and performs no minting or message-serial stamping.
- [x] #3 core.Channel.AppendChannelMessage(ctx, msgs) -> (*protocol.ChannelMessage, bool, error) is added: it calls the underlying storage.ChannelStore (which mints + persists) and on a non-idempotent return links the cm into the live list before returning.
- [x] #4 core.Manager takes a storage.Storage on construction; GetChannel(name) constructs Channels backed by storage.Channel(name). NewManager-style constructors used by main.go and tests are updated accordingly.
- [x] #5 internal/realtime (connection.handleMessage) and internal/rest (server.HandlePublish) publish paths route the publish through Channel.AppendChannelMessage; on a non-nil error from the storage round-trip the path NACKs (realtime) or returns 5xx (rest) instead of ACKing/201.
- [x] #6 cmd/ably-server wires a storage.Storage (default: in-memory) into the manager at startup.
- [x] #7 All existing test suites pass under go test ./... — including the unit tests for core, realtime, rest, and the storage/memory + storage/bbolt contract suites in storagetest.
- [x] #8 DESIGN.md §5.1 (Channel sketch), §6 (storage interface signature), and §7.1 (single-process publish sequence) are updated to reflect storage as the sole serial authority and Channel.Append(cm) as the link-only API.
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Refactor core.Channel: drop the gen field; add a store storage.ChannelStore field; change Append's signature to Append(cm *protocol.ChannelMessage) doing just the linked-list link + notify; add AppendChannelMessage(ctx, msgs) that calls store.AppendChannelMessage and on a non-idempotent return calls c.Append(cm).
2. Refactor core.Manager: drop seriesID and now (the generator config now lives inside the storage backend); take a storage.Storage in the constructor; GetChannel(name) builds a Channel with store.Channel(name). Update NewManager / NewManagerWithClock to fit the new shape — NewManager(store), and a NewManagerForTest helper or similar that keeps deterministic-clock testing easy.
3. Update core unit tests: channel_test.go's newTestChannel constructs a Channel via a memory.New backend with a deterministic SeriesID/Now so the serial-format assertions still hold. Migrate Append(msgs...) call sites to AppendChannelMessage(ctx, msgs) or to a thin test helper that does store+Append. manager_test.go updates to construct manager with a memory.New backend.
4. Update internal/realtime/connection.handleMessage: call ch.AppendChannelMessage(ctx, msg.Messages); on error log + NACK; otherwise ACK as today (Count = len(msg.Messages); idempotent return still ACKs because the publish was accepted, just deduped). Update realtime test harness (newTestServer) to construct the manager with a memory storage.
5. Update internal/rest/server.HandlePublish: call ch.AppendChannelMessage(r.Context(), msgs); on error return 500; otherwise 201 as today. Update rest test harness similarly.
6. Wire storage into cmd/ably-server/main.go: construct memory.New(memory.Options{}) and pass to core.NewManager. (Keeps the existing CLI surface untouched — disk/cluster modes come in later tasks.)
7. Update DESIGN.md: §5.1 sketch drops gen, switches Append to Append(cm), adds AppendChannelMessage. §6 ChannelStore signature updated to (ctx, msgs) -> (cm, idempotent, err). §7.1 step 3 dropped (no separate mint step) and step 4 reworded so storage mints+returns the cm.
8. Run go test ./... and fix any cascading test failures.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Plan recorded; barrelling into implementation per user direction (no approval gate).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Storage is now the sole authority for channelSerial minting; core.Channel has been demoted to a pure linked-list maintainer that links already-minted ChannelMessages. Aligns the code with DESIGN.md §6/§8 and unblocks 'always expose a channelSerial' (TASK-16) and 'ACK only after storage write' (TASK-20).

## Why

Two paths were minting channelSerials in parallel: core.Channel.Append via core.Manager's per-channel serial.Generator, and storage.ChannelStore.AppendChannelMessage via its own backend generator. Only the latter persisted generator monotonic state across restarts (bbolt _meta), so the publish path was effectively bypassing the bbolt-restored state. Storage was also already documented as the sole minter in DESIGN §6/§8.

## Changes

- internal/core/channel.go — Channel.Append now takes (cm *protocol.ChannelMessage) and does nothing but link + notify; AppendChannelMessage(ctx, msgs) added, delegating to store.AppendChannelMessage and linking the returned cm on non-idempotent returns. gen field replaced with store storage.ChannelStore.
- internal/core/manager.go — NewManager(store storage.Storage) replaces the zero-arg constructor; the seriesID + per-channel generator construction is gone (lives in the storage backend now). NewManagerWithClock removed — clock injection now belongs in the backend's Options.
- internal/realtime/connection.go — handleMessage routes the publish via Channel.AppendChannelMessage and NACKs on storage error (previously always ACKed).
- internal/rest/server.go — HandlePublish routes the same way and returns 500 on storage error.
- cmd/ably-server/main.go — wires memory.New(memory.Options{}) into the manager as the default backend.
- DESIGN.md §5.1 / §5.2 / §6 / §7.1 — sketches updated to match: storage.ChannelStore.AppendChannelMessage signature corrected to (ctx, msgs) → (cm, idempotent, err); Channel sketch drops the generator and switches to Append(cm) + AppendChannelMessage(ctx, msgs); publish-flow steps in §5.2 and §7.1 reworded to reflect storage-mints-and-returns.

## Tests

- go test ./... — all suites pass (core, realtime, rest, storage/memory, storage/bbolt).
- go test -race ./... — clean.
- go vet ./... — clean.

Core unit-test fixtures (newTestChannel, newTestManager) now construct Channels via a deterministic memory.New backend. The concurrent-Append test switched to empty Message.IDs to avoid the storage layer's idempotency dedup, preserving the original intent (proving the linked-list can serialise N concurrent appends).

## Follow-ups

- TASK-16 / TASK-20 are now unblocked — they consume the (cm, idempotent, err) return that this PR exposes.
<!-- SECTION:FINAL_SUMMARY:END -->
