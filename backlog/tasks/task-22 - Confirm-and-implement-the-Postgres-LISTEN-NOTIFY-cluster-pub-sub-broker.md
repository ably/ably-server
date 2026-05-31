---
id: TASK-22
title: Confirm and implement the Postgres LISTEN/NOTIFY cluster pub/sub broker
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 16:27'
updated_date: '2026-05-31 21:54'
labels: []
dependencies:
  - TASK-21
ordinal: 22000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Cross-node fan-out for cluster mode: a publish on any node must reach attachments on every other node. First confirm the approach in DESIGN §7.2 (and with the team if anything is unsettled), then implement it in internal/cluster.

Per §7.2: each node runs a single goroutine on a dedicated Postgres connection that LISTENs for channel-publish notifications. The publishing node INSERTs the channel_messages/messages rows (storage) and emits NOTIFY ably_channel '<channel>:<channelSerial>'. Every listening node (including the publisher) receives the notification, fetches the canonical row by (channel, channelSerial), and calls channel.Append(cm) on its local Channel — deduplicating by (channel, channelSerial) because NOTIFY is at-least-once. Send pointers (channel + serial), never payloads, to stay under NOTIFY's 8KB limit. Global ordering falls out of the serial format (the seriesId disambiguates same-millisecond serials across nodes). Single-process modes (memory/disk) bypass the broker entirely. Depends on the Postgres storage implementation.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 storage.Storage.Channel takes an Appender at construction: Channel(name string, appender Appender) ChannelStore. The Appender is invoked by the backend once per fresh (non-idempotent) commit; idempotent returns do not fire the appender because the original was delivered when first persisted.
- [x] #2 storage.ChannelStore.AppendChannelMessage is renamed to Store. The two-step (persist + caller-driven Append) is gone — Store is the only call publishes make, and the link onto the linked list arrives via the Appender.
- [x] #3 Memory and bbolt backends fire the appender synchronously inside Store after their respective commits (bbolt fires after the bolt tx returns, outside the write lock). Nil appender is handled gracefully — used by the storagetest contract suite.
- [x] #4 postgres.Storage runs a long-lived LISTEN goroutine on a dedicated pgx.Conn opened at Open(). Store() emits a NOTIFY inside the publish tx with payload JSON {channel, serial}. The LISTEN loop receives every NOTIFY (including the publisher's own), looks up the registered channelStore by name, fetches the canonical cm via the pool, and calls appender.Append(cm). No self-dedup; the publisher does not link synchronously.
- [x] #5 Notifications for channels with no local registration (no Channel(name, …) call) are dropped; the canonical cm remains in storage for a future ATTACH+resume.
- [x] #6 core.Manager regains a storage.Storage field; NewManager(store) is the constructor. GetChannel(name) creates a Channel, calls store.Channel(name, channel) so the Channel itself is the Appender, and wires the returned ChannelStore into the Channel for Publish.
- [x] #7 core.Channel exposes Publish(ctx, msgs) (delegates to store.Store) and Append(cm) (satisfies Appender). Realtime and REST handlers lose their storage.Storage field — they call manager.GetChannel(name).Publish(ctx, msgs) only.
- [x] #8 Integration test TestPostgresClusterBrokerDeliversCrossNode brings up two postgres.Storage instances against the same schema, registers a recording Appender on each, publishes via one node, and asserts both appenders receive the cm exactly once (no duplicates from the publisher's own NOTIFY). Plus the existing contract / bootstrap / migrate tests pass unchanged.
- [x] #9 DESIGN.md §5.1 / §5.2 / §6 / §7 (both sub-sections) rewritten to describe the unified Appender flow: same Publish API across modes, single delivery path via storage.Appender.Append, no self-dedup in cluster mode, ~1-5ms local-visibility round-trip as the explicit trade-off.
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Extend postgres backend (internal/storage/postgres/postgres.go):
   - Add SeriesID() method exposing the per-process seriesId.
   - Add channelStore.LoadChannelMessage(ctx, serial) — query messages by (channel, channel_serial) ORDER BY idx, decode payload rows into one cm.
   - In AppendChannelMessage, after the per-message INSERTs and before Commit, exec NOTIFY ably_channel, $payload where payload is JSON({channel, serial}).
2. Create internal/cluster package with broker.go:
   - Options{DSN string; Store *postgres.Storage; Manager *core.Manager; Logger *slog.Logger}
   - Broker type with New(opts), Start(ctx), Stop().
   - Start opens a dedicated pgx.Conn (NOT from the pool — pgx requires a long-lived single conn for LISTEN), execs LISTEN ably_channel, and spawns a goroutine that loops on conn.WaitForNotification(ctx).
   - On notification: json.Unmarshal payload → (channel, serial). Extract seriesId after the '@' in serial. If seriesId == broker's own seriesId, return (self-publish already linked synchronously by the publish path). Otherwise call store.Channel(channel).LoadChannelMessage(ctx, serial) → cm; manager.GetChannel(channel).Append(cm).
   - Stop cancels the loop context and closes the dedicated conn.
3. Wire main.go:
   - Add --mode=cluster + --db-dsn flags.
   - Extend openStorage to handle the 'cluster' case (returns *postgres.Storage instead of storage.Storage).
   - In run(), if cluster mode, also start a cluster.Broker with the dedicated conn DSN; defer Stop() before storage Close.
4. Integration test (internal/cluster/broker_test.go, //go:build integration):
   - Use testcontainers (already in go.sum) — but spawn from cluster package's own test helper; reuse the per-schema-isolation pattern from postgres_test.go.
   - Two (postgres.Storage, core.Manager, cluster.Broker) tuples on the same schema (different seriesIds, different DSN sessions).
   - Subscribe on node A; publish on node B; assert node A sees the cm within 2s.
   - Subscribe on node A; publish on node A; assert node A sees the cm exactly once (no duplicate from its own NOTIFY round-trip).
5. DESIGN §7.2 update:
   - Replace the 'sketch' tone with implemented detail: NOTIFY payload format, seriesId-based dedup, dedicated-conn LISTEN semantics.
   - Note the disconnect/missed-NOTIFY catch-up concern as deferred (future task).
6. Verify: go build, go vet, go test ./..., go test -race ./..., go test -tags=integration ./...
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Dedup design: rely on seriesId comparison (cheap, no state). The publisher's local Append happens synchronously after store commit; its own NOTIFY arrives via the listener and is skipped because its seriesId matches. Foreign NOTIFYs fall through to fetch+link.

Trade-off acknowledged: LISTEN/NOTIFY is not durable — if the dedicated LISTEN conn drops, notifications during the gap are lost. Recovering missed events needs a reconciliation pass (e.g., poll storage for serials > last-seen on reconnect). Out of TASK-22 scope; left as a follow-up.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Implements the Postgres LISTEN/NOTIFY broker as an internal piece of the postgres storage backend and reshapes the publish path across all modes into a single symmetric flow.

## Architectural pivot mid-task

The original sketch (broker as a separate internal/cluster package, publisher Appends synchronously, broker dedups its own NOTIFYs by seriesId) had two paths into core.Channel.Append — one synchronous for own publishes, one async for foreign publishes — and required exporting NotifyChannelName/NotifyPayload/SeriesID/LoadChannelMessage from the postgres package purely for the broker's use. After review the symmetric design landed:

- All cms reach core.Channel.Append via a single delivery path: the storage backend invokes the registered Appender after a successful commit.
- Memory/bbolt fire the Appender synchronously, in-process.
- Postgres fires the Appender exclusively from a LISTEN goroutine — including for the publisher's own publishes. No self-dedup.

The publisher pays a ~1–5ms NOTIFY round-trip for local visibility in cluster mode; in exchange, every backend has one delivery mechanism and the postgres exports for cross-package broker plumbing go away.

## Shape

- storage.Appender interface: Append(cm).
- storage.Storage.Channel(name, appender) — Channel takes an Appender at construction; storage memoises by name.
- storage.ChannelStore.Store renamed from AppendChannelMessage. Idempotent returns do not fire the Appender (original was delivered on first persist).
- core.Manager.NewManager(store) — Manager re-acquires storage. GetChannel(name) creates a Channel, calls store.Channel(name, ch) so the Channel itself is the Appender, and wires the returned ChannelStore.
- core.Channel.Publish(ctx, msgs) — orchestrates the publish; delegates to store.Store. Channel.Append(cm) satisfies the Appender interface and remains the only writer to the linked list.
- Realtime / REST handlers drop their storage.Storage field — they only hold a *core.Manager and call manager.GetChannel(name).Publish(...) / .Attach().
- postgres.Storage opens a dedicated pgx.Conn for LISTEN at Open(), runs a goroutine that loops on WaitForNotification, parses the JSON payload, looks up the registered channelStore by name, fetches the canonical cm by (channel, channel_serial), and calls appender.Append(cm). Notifications for channels with no local registration are dropped (canonical cm remains in storage).
- Store() in postgres emits pg_notify inside the publish tx — PG buffers until commit so listeners only see committed publishes.

## Tests

- TestPostgresClusterBrokerDeliversCrossNode (new, //go:build integration): two postgres.Storage instances on the same schema; publish via one; assert both registered Appenders receive the cm exactly once.
- TestPostgresChannelStoreContract / TestPostgresBootstrapIsIdempotent / TestPostgresMigrateIsConcurrentSafe pass against the new shape.
- storagetest contract suite updated (Channel(name, nil); Store rename).
- core / realtime / rest test harnesses updated.
- go build, go test ./..., go test -race ./..., go vet ./..., go test -tags=integration ./... — all clean.

## DESIGN updates

§5.1 rewritten: Channel is paired with its ChannelStore at construction; Channel.Publish delegates to store, Channel.Append is the Appender. §5.2 reflects the new publish path. §6 explains the Appender contract and per-backend invocation semantics (sync for memory/bbolt, LISTEN-driven for postgres). §7 collapsed into a single unified flow with sub-sections describing where the Appender fires from in each mode.

## Follow-ups

- LISTEN/NOTIFY drops events during conn loss. Reconciling missed cms via a post-reconnect history scan is out of TASK-22 scope and remains a follow-up (mentioned in §7.2).
- main.go does not yet expose --mode=cluster + --db-dsn flags. That's the next-step plumbing for the binary to actually use the broker; not strictly part of TASK-22 (which is about the broker itself working).
<!-- SECTION:FINAL_SUMMARY:END -->
