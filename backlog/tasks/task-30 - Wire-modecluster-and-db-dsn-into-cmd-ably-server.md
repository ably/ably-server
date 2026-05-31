---
id: TASK-30
title: Wire --mode=cluster and --db-dsn into cmd/ably-server
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 21:58'
updated_date: '2026-05-31 22:14'
labels: []
dependencies:
  - TASK-22
ordinal: 30000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
TASK-22 built the Postgres LISTEN/NOTIFY broker as part of postgres.Storage, but cmd/ably-server/main.go's openStorage helper only knows 'memory' and 'disk'. Add the cluster branch + the --db-dsn flag so the binary can actually be run against Postgres and exercise the broker.

This is the small piece of CLI plumbing that unblocks running ably-server against a real PG instance (and is a prerequisite for the multi-server integration test in TASK-25).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 main.go accepts --mode=cluster and --db-dsn <libpq DSN>; an ABLY_SERVER_DB_DSN env var fills in --db-dsn when the flag is empty (matching the --api-key/ABLY_SERVER_API_KEY pattern).
- [x] #2 openStorage's switch gains a 'cluster' branch that calls postgres.Open(ctx, postgres.Options{DSN: dbDSN}). The function signature is extended to take ctx and dbDSN; existing memory/disk callers are updated.
- [x] #3 Cluster mode without --db-dsn fails startup with a clear error logged via slog (no panic, exit code 1).
- [x] #4 The store's Close (which now stops the LISTEN goroutine and closes the dedicated conn before releasing the pool — see TASK-22) is called as part of the graceful-shutdown path after srv.Shutdown returns.
- [x] #5 go run ./cmd/ably-server -h shows the new --mode value and --db-dsn flag with descriptions; go build ./..., go vet ./..., go test ./... remain clean.
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Adds the small piece of CLI wiring needed for cmd/ably-server to actually use the Postgres backend (and the TASK-22 broker).

## What's in

- main.go: --mode now accepts 'cluster'; new --db-dsn flag with ABLY_SERVER_DB_DSN env fallback (matching --api-key/ABLY_SERVER_API_KEY).
- openStorage's signature extended to (ctx, mode, dataDir, dbDSN); the new 'cluster' branch calls postgres.Open(ctx, postgres.Options{DSN: dbDSN}). The function-level doc-comment lists all three modes.
- Cluster mode without --db-dsn fails startup with a clear slog.Error and exit code 1 (no panic).
- store.Close continues to fire via defer, after srv.Shutdown returns — which for postgres also stops the LISTEN goroutine and closes the dedicated conn (TASK-22). A clarifying comment notes the ordering.

## Verification

- go build, go vet, go test ./..., go test -race ./... — all clean.
- go run ./cmd/ably-server -h surfaces --mode {memory, disk, cluster} and --db-dsn with descriptions.
- go run ./cmd/ably-server --api-key=… --mode=cluster (no DSN) → 'open storage err="--db-dsn is required when --mode=cluster (env: ABLY_SERVER_DB_DSN)"' and exits 1.

## Why

TASK-22 built the broker but the binary couldn't actually be run against Postgres. With this in place, ably-server --mode=cluster --db-dsn=… boots, applies migrations, runs the LISTEN goroutine, and serves WS + REST against the shared DB. Prerequisite for the cmd-level integration test (TASK-24) and the multi-server cluster test (TASK-25).
<!-- SECTION:FINAL_SUMMARY:END -->
