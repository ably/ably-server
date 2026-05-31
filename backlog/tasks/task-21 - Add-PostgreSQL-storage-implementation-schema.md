---
id: TASK-21
title: Add PostgreSQL storage implementation + schema
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 16:27'
updated_date: '2026-05-31 18:27'
labels: []
dependencies:
  - TASK-13
ordinal: 21000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Implement the storage.Storage / storage.ChannelStore interfaces for PostgreSQL (cluster mode, DESIGN §6.3) in a new internal/storage/postgres package. Create the schema per the §6.3 sketch: channels; channel_messages (PK (channel, channel_serial), one row per atomic publish); messages (PK (channel, channel_serial, idx), FK -> channel_messages ON DELETE CASCADE, plus a unique index on (channel, id) WHERE id IS NOT NULL for idempotency). AppendChannelMessage persists one ChannelMessage + its Messages atomically and enforces id-based idempotency via the unique index; History runs a bounded forward range scan ordered by channel_serial. Mint channelSerials in the backend and persist generator monotonic state across restarts (the Postgres equivalent of the bbolt _meta state, DESIGN §8). Exercise it against the shared contract suite in internal/storage/storagetest. Retention (TTL expiry) and the per-channel message cap are handled as a cross-backend concern in a separate task. Depends on the serial-generation consolidation (storage owns minting).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 A new internal/storage/postgres package implements storage.Storage and storage.ChannelStore using github.com/jackc/pgx/v5 with a pgxpool.Pool.
- [x] #2 History runs a single ORDER BY channel_serial range query bounded by AfterChannelSerial / Limit, decoding each row's msgpack payload back into a protocol.Message and grouping by channel_serial; HasMore is set when Limit truncates the result.
- [x] #3 Each protocol.Message is stored as a msgpack-encoded blob in messages.payload; the id column is a separate (nullable) column used only by the idempotency UNIQUE index; round-trip preserves all protocol.Message fields.
- [x] #4 Per-process seriesId is generated at Open() and NOT persisted in Postgres — multi-node deployments rely on distinct per-node seriesIds (DESIGN §8); restart monotonicity is naturally satisfied because a fresh seriesId produces non-colliding serials.
- [x] #5 internal/storage/postgres/postgres_test.go is build-tagged //go:build integration; it spins up a Postgres testcontainer once per test run and exercises storagetest.RunChannelStoreTests against it. Plain go test ./... ignores it; go test -tags=integration ./... runs it.
- [x] #6 go.mod gains github.com/jackc/pgx/v5 and github.com/testcontainers/testcontainers-go (postgres module). go build ./..., go test ./..., go vet ./... remain clean without the integration tag.
- [x] #7 Open(ctx, Options{DSN, Now}) connects, applies the embedded schema (single messages table + the partial UNIQUE idempotency index) using CREATE IF NOT EXISTS so it is safe on already-bootstrapped databases; concurrent-safe across N booting nodes is TASK-23's concern.
- [x] #8 DESIGN.md §6.3 is updated to reflect the shipped schema: one messages table, no separate channels or channel_messages tables, no created_at column (retention uses the timestamp embedded in channel_serial).
- [x] #9 AppendChannelMessage takes a per-channel pg_advisory_xact_lock(hashtext(channel)) inside the transaction, pre-checks any contained Message.IDs for prior matches (returns the originally-persisted cm with idempotent=true on hit), otherwise mints a channelSerial via the in-process serial.Generator, stamps each Message.Serial, and inserts one row per contained Message into the messages table.
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add deps: github.com/jackc/pgx/v5 (+ pgxpool) and github.com/testcontainers/testcontainers-go/modules/postgres. go mod tidy.
2. Create internal/storage/postgres/schema.sql with the CREATE IF NOT EXISTS DDL for channels, channel_messages, messages, plus the partial UNIQUE idempotency index. Embed via go:embed.
3. Create internal/storage/postgres/postgres.go:
   - Options{DSN string; Now func() int64}
   - Storage{pool *pgxpool.Pool; gen *serial.Generator; mu sync.Mutex; channels map[string]*channelStore}
   - Open(ctx, opts) — pgxpool.New, ping, exec(schema), construct generator with a fresh seriesId.
   - Close — pool.Close.
   - Channel(name) — memoised channelStore lookup.
   - channelStore.AppendChannelMessage:
       BEGIN; SELECT pg_advisory_xact_lock(hashtext($1));
       pre-check ids → load+return original on hit;
       gen.Mint() → cm; stamp Message.Serial;
       INSERT INTO channels ON CONFLICT DO NOTHING;
       INSERT INTO channel_messages (channel, channel_serial);
       per-message INSERT INTO messages (channel, channel_serial, idx, NULLIF(id, ''), msgpack(payload));
       COMMIT.
   - channelStore.History: a single SELECT … ORDER BY channel_serial, idx … LIMIT $, grouping rows by channel_serial in-memory; set HasMore when (Limit+1) rows arrive (i.e. fetch Limit+1 then trim).
4. Create internal/storage/postgres/postgres_test.go with //go:build integration:
   - Package-level testcontainer setup (sync.Once) bringing up postgres:16-alpine via testcontainers-go, returning a DSN; container is torn down at TestMain end.
   - Factory func returns a fresh storage.Storage per subtest, but the DB is shared — each subtest uses a fresh schema name (CREATE SCHEMA test_XYZ; SET search_path = test_XYZ) or a fresh DB to keep tests isolated.
   - Call storagetest.RunChannelStoreTests(t, factory).
5. Optionally add a 'restart' test analogous to TestBBoltSurvivesProcessRestart — open, publish, Close, reopen, History — verifies bootstrap is idempotent on a pre-populated DB.
6. Update DESIGN §6.3 to match the shipped schema (payload BYTEA; no per-field message columns; no schema_migrations; no shared serial_state).
7. Verify: go build ./..., go test ./..., go vet ./... pass without the tag; go test -tags=integration ./internal/storage/postgres/... passes with Docker available.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Plan recorded. Driver: pgx/v5 (chosen for LISTEN/NOTIFY support coming in TASK-22). Schema isolation in tests: per-test schema name with SET search_path; cheaper than fresh DBs. Integration test build tag matches what TASK-24 expects so the binary-level test can reuse the testcontainer harness.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
New internal/storage/postgres package implementing storage.Storage and storage.ChannelStore against a PostgreSQL backend. Cluster mode now has a real backend it can target; foundational for the LISTEN/NOTIFY broker (TASK-22), auto-migrate (TASK-23), and the single/multi-server integration tests (TASK-24/25).

## What's in

- internal/storage/postgres/postgres.go — Storage + channelStore on pgx/v5 + pgxpool. Open(ctx, Options) dials Postgres, pings, applies the embedded schema, and constructs the serial.Generator with a fresh per-process seriesId.
- internal/storage/postgres/schema.sql — channels, channel_messages, messages tables with the partial UNIQUE idempotency index on (channel, id) WHERE id IS NOT NULL. All CREATEs use IF NOT EXISTS so Open is safe on already-bootstrapped DBs. (Concurrent-safe bootstrap across N booting nodes is TASK-23.)
- internal/storage/postgres/postgres_test.go (//go:build integration) — testcontainers-go spins up postgres:17-alpine once per test run; each subtest gets a fresh schema via search_path so the contract suite's 'fresh storage' contract holds without per-test databases. Exercises storagetest.RunChannelStoreTests + a bootstrap-idempotent smoke test.

## How publish works

AppendChannelMessage opens a tx, takes pg_advisory_xact_lock(hashtext(channel)) — released automatically at commit/rollback — pre-checks any non-empty Message.IDs (idempotent return on hit), then mints a channelSerial via the in-process serial.Generator, stamps each Message.Serial, lazily upserts channels, INSERTs the channel_messages row, and per-message INSERTs into messages with the protocol.Message msgpack-encoded into the payload column. The advisory lock serialises writers on the same channel across all processes sharing this DB.

History runs a single CTE-bounded query: pick the next Limit+1 channel_serials past AfterChannelSerial, join messages, ORDER BY channel_serial, idx. Rows are grouped into ChannelMessages in Go; HasMore is set when the Limit+1 fetch returned more cms than requested.

## Why msgpack payload + extracted id column

Faithful round-trip of protocol.Message (including the awkward Data any field) without writing per-field marshallers, while still giving the idempotency index its own indexable id column. Future work that wants per-field SQL queries (e.g. by client_id) can split additional columns out without breaking existing rows.

## seriesId is per-process

Per DESIGN §8: multi-node deployments rely on distinct per-node seriesIds to disambiguate concurrent mints. Open() generates a fresh seriesId each time; nothing is persisted in PG. Restart monotonicity is satisfied automatically because the new seriesId produces non-colliding serials.

## DESIGN updates

§6.3 schema sketch refreshed to match the shipped DDL: payload BYTEA replaces the per-field message columns, channel_messages.timestamp renamed to created_at, and a paragraph added on advisory locking + per-process seriesIds.

## Deps

- github.com/jackc/pgx/v5 (+ pgxpool) — picked over lib/pq for active maintenance and native LISTEN/NOTIFY support arriving in TASK-22.
- github.com/testcontainers/testcontainers-go/modules/postgres — used only by the integration test; the wider testcontainers-go module is a transitive dep.

## Tests

- go test ./... — passes without -tags=integration (suite skips the build-tagged Postgres tests).
- go test -race ./... — clean.
- go vet ./... — clean.
- go test -tags=integration ./internal/storage/postgres/... — passes against a fresh postgres:17-alpine container, ~36s end to end.

## Follow-ups left explicitly to other tasks

- TASK-23: concurrent-safe bootstrap (pg_advisory_lock around schema apply) and a schema_migrations tracker for forward migrations.
- TASK-22: LISTEN/NOTIFY broker for cluster pub/sub.
- TASK-26: retention sweep (the created_at index is already in place).

## Amendment

After review the original three-table schema (channels, channel_messages, messages) was collapsed to a single messages table — the channels table was dead weight (no FK, no reads, no metadata) and channel_messages added nothing the messages rows didn't already carry. The created_at column was also removed: retention can use the timestamp embedded in channel_serial (the first 14 chars are zero-padded ms-since-epoch, so lex order matches numeric order), so a range scan over the PK covers ordered history reads AND time-based retention without a separate column or index. Final schema: one messages table, two indexes (PK + partial UNIQUE for idempotency).
<!-- SECTION:FINAL_SUMMARY:END -->
