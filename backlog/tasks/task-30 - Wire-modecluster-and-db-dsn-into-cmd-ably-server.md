---
id: TASK-30
title: Wire --mode=cluster and --db-dsn into cmd/ably-server
status: To Do
assignee: []
created_date: '2026-05-31 21:58'
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
- [ ] #1 main.go accepts --mode=cluster and --db-dsn <libpq DSN>; an ABLY_SERVER_DB_DSN env var fills in --db-dsn when the flag is empty (matching the --api-key/ABLY_SERVER_API_KEY pattern).
- [ ] #2 openStorage's switch gains a 'cluster' branch that calls postgres.Open(ctx, postgres.Options{DSN: dbDSN}). The function signature is extended to take ctx and dbDSN; existing memory/disk callers are updated.
- [ ] #3 Cluster mode without --db-dsn fails startup with a clear error logged via slog (no panic, exit code 1).
- [ ] #4 The store's Close (which now stops the LISTEN goroutine and closes the dedicated conn before releasing the pool — see TASK-22) is called as part of the graceful-shutdown path after srv.Shutdown returns.
- [ ] #5 go run ./cmd/ably-server -h shows the new --mode value and --db-dsn flag with descriptions; go build ./..., go vet ./..., go test ./... remain clean.
<!-- AC:END -->
