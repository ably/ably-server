---
id: TASK-60
title: Add Docker Compose config to run PostgreSQL + 3 ably-server processes
status: To Do
assignee: []
created_date: '2026-06-16 18:14'
labels:
  - performance
dependencies: []
ordinal: 60000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Provide a Docker Compose stack that brings up the full cluster-mode topology locally for performance and integration work: one PostgreSQL instance plus three ably-server processes all sharing it.

This gives us a reproducible local cluster matching the `cluster` deployment mode (DESIGN.md §6.2/§7.2) — three processes over a shared Postgres for both state and LISTEN/NOTIFY pub/sub — that the benchmark program (separate task) can target.

Notes / pointers:
- ably-server is built from `cmd/ably-server`; relevant flags are `--mode=cluster`, `--db-dsn` (env `ABLY_DB_DSN`), `--listen` (default `:8080`), and `--api-key` (env `ABLY_API_KEY`). Auto-migrate runs on startup under a Postgres advisory lock (TASK-23), so all three can boot against an empty DB concurrently.
- Each server should expose a distinct host port; a shared api-key across all three so a client can hit any node.
- Add a Dockerfile (or reuse a build stage) for the server binary if one doesn't already exist.
- Wait for Postgres health (`/readyz` does a DB ping in cluster mode) before considering the stack up.
- The existing cluster integration tests in `cmd/ably-server/integration_test.go` are a good reference for the DSN/flag wiring.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 docker compose up brings up Postgres + 3 ably-server nodes in cluster mode against the shared DB
- [ ] #2 Each node is reachable on its own host port and shares one api-key
- [ ] #3 Stack comes up cleanly from empty DB (concurrent auto-migrate) with no manual setup steps
<!-- AC:END -->
