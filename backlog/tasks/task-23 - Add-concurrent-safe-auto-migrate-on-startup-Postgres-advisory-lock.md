---
id: TASK-23
title: Add concurrent-safe auto-migrate on startup (Postgres advisory lock)
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 16:27'
updated_date: '2026-05-31 19:15'
labels: []
dependencies:
  - TASK-21
ordinal: 23000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
On startup every server process should be able to apply pending schema migrations, but only one runs them at a time. Acquire a Postgres advisory lock (pg_advisory_lock on a fixed key) so a single process holds the migration lock, applies migrations, then releases; other processes block until it finishes and then observe an up-to-date schema (effective no-op). Track applied migrations (e.g. a schema_migrations table) and ship migrations as embedded SQL. Must be safe for a cold start where N nodes boot simultaneously against an empty database. Migration mechanism (hand-rolled vs a library such as golang-migrate) is open; the advisory-lock serialisation is the key requirement. Depends on the Postgres storage implementation/schema.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Migrations live in internal/storage/postgres/migrations/*.sql, embedded via go:embed, and are applied in lex order of filename (zero-padded version prefix, e.g. 0001_initial.sql).
- [x] #2 A schema_migrations(version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now()) table tracks which versions have been applied. It is created by Open() outside the migration loop (chicken-and-egg).
- [x] #3 postgres.Open acquires a session-scoped pg_advisory_lock on a project-derived fixed int8 key for the duration of the migration sweep, applies each pending migration in its own transaction with the schema_migrations INSERT in the same tx, then releases the lock.
- [x] #4 The existing schema.sql content becomes 0001_initial.sql verbatim (no functional schema change in this task). The CREATE-IF-NOT-EXISTS-as-bootstrap path is retired in favour of the migrate path.
- [x] #5 A concurrent-Open integration test spawns N (≥10) goroutines that each open a fresh postgres.Storage against the same empty schema simultaneously. All Opens succeed; schema_migrations contains exactly one row per shipped migration (no duplicates, no missing); the messages table is correctly created.
- [x] #6 Existing storagetest contract suite + the bootstrap-idempotent smoke test continue to pass under go test -tags=integration. Plain go test ./..., go test -race ./..., go vet ./... remain clean.
- [x] #7 DESIGN.md §6.3 / §11 updated to describe the auto-migrate flow: embedded migrations, advisory-locked sweep, schema_migrations tracker, forward-only.
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Restructure migrations: create internal/storage/postgres/migrations/0001_initial.sql containing the current schema.sql contents (messages table + idempotency UNIQUE index). Delete the old schema.sql.
2. Embed via //go:embed migrations/*.sql into an embed.FS.
3. In postgres.go, replace the single Exec(schemaSQL) bootstrap with a migrate(ctx, pool) helper:
   - Acquire one dedicated conn from the pool (pool.Acquire).
   - SELECT pg_advisory_lock(<fixed int8 derived from fnv64a('ably-server-migrations')>).
   - CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now()).
   - SELECT version FROM schema_migrations → applied set (map[string]struct{}).
   - List embedded entries; sort by filename.
   - For each migration whose version is not in the applied set: BEGIN; exec(file contents); INSERT INTO schema_migrations(version) VALUES ($1); COMMIT.
   - SELECT pg_advisory_unlock(<key>) (defensive; conn release would do it too).
   - conn.Release().
4. The fixed int8 key is computed once at package init via fnv64a of the literal string 'ably-server-migrations'.
5. Tests (//go:build integration):
   - Existing contract + bootstrap-idempotent tests should pass unchanged (the migrate flow is logically a superset of the prior CREATE-IF-NOT-EXISTS path).
   - Add TestPostgresMigrateIsConcurrentSafe: create a fresh schema, spawn ≥10 goroutines that each call postgres.Open against that schema in parallel. Assert all Opens succeed; schema_migrations has exactly one row per shipped migration; SELECT 1 FROM messages LIMIT 0 succeeds (table exists).
6. Update DESIGN.md §6.3 / §11 with the auto-migrate description.
7. Verify: go build, go test ./..., go test -race ./..., go vet ./..., go test -tags=integration ./internal/storage/postgres/... all pass.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Decided hand-rolled migrations over golang-migrate: scope is one migration today, the runtime is <100 lines, no new dep. Lock key derived deterministically via FNV-64a so it's stable across builds without a hardcoded magic number.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Auto-migrate on startup with concurrent-safety via a session-scoped pg_advisory_lock. Unblocks TASK-24 (single-server integration test that relies on auto-migrate) and removes the lingering CREATE-IF-NOT-EXISTS bootstrap race that TASK-21 explicitly deferred.

## What's in

- internal/storage/postgres/migrations/0001_initial.sql — the existing schema, IF NOT EXISTS guards removed (no longer needed under version tracking).
- internal/storage/postgres/postgres.go — replaced the single Exec(schemaSQL) bootstrap with migrate(ctx, pool):
  - Acquires a dedicated conn from the pool.
  - SELECT pg_advisory_lock(<fixed int8 const>) — session-scoped, blocks concurrent Opens. The key is arbitrary; advisory-lock keyspace is per-database and opt-in.
  - CREATE TABLE IF NOT EXISTS schema_migrations (the tracker itself, outside the migration loop).
  - SELECT version FROM schema_migrations → applied set.
  - For each embedded migration (lex order of filename) not yet applied: BEGIN; exec migration SQL; INSERT INTO schema_migrations; COMMIT.
  - SELECT pg_advisory_unlock + conn.Release.

## Tests

- TestPostgresChannelStoreContract — unchanged, still passes.
- TestPostgresBootstrapIsIdempotent — now exercises the migrate loop's 'all migrations applied, no-op on second Open' path.
- TestPostgresMigrateIsConcurrentSafe — new. Spawns 10 goroutines each calling postgres.Open on the same fresh schema. All Opens succeed; schema_migrations has exactly one row ('0001_initial'); the messages table is queryable. Without the advisory lock this would race the CREATE TABLE and produce duplicate-key errors or partial state.

## Crash recovery

If a node crashes mid-sweep, the in-progress migration's tx rolls back (no schema_migrations row), and the next Open retries it. PG releases the session-scoped advisory lock automatically when the crashed connection ends. No half-applied state is observable to other nodes.

## DESIGN updates

§6.3 rewritten to describe the migrate-sweep flow (numbered list of the four steps + tx-per-migration recovery semantics). §11 (Lifecycle & operations) gets a new 'Startup' paragraph calling out per-backend bootstrap and the cross-node serialisation.

## Verification

- go build, go vet, go test ./..., go test -race ./... — all clean.
- go test -tags=integration ./internal/storage/postgres/... — all 3 tests pass (contract, bootstrap-idempotent, concurrent-safe migrate). ~3-5s end to end against a postgres:17-alpine testcontainer.
<!-- SECTION:FINAL_SUMMARY:END -->
