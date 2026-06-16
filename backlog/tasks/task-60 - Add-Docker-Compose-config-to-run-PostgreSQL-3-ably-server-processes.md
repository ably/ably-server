---
id: TASK-60
title: Add Docker Compose config to run PostgreSQL + 3 ably-server processes
status: Done
assignee:
  - '@claude'
created_date: '2026-06-16 18:14'
updated_date: '2026-06-16 18:22'
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
- [x] #1 docker compose up brings up Postgres + 3 ably-server nodes in cluster mode against the shared DB
- [x] #2 Each node is reachable on its own host port and shares one api-key
- [x] #3 Stack comes up cleanly from empty DB (concurrent auto-migrate) with no manual setup steps
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Added Dockerfile (multi-stage, static CGO_ENABLED=0 binary on alpine), .dockerignore, and docker-compose.yml defining one postgres:16-alpine + three ably-server nodes (node1/2/3 on host ports 8081/8082/8083). Nodes share api-key app.key:secret and DSN postgres://ably:ably@postgres:5432/ably via ABLY_SERVER_API_KEY / ABLY_SERVER_DB_DSN; a YAML anchor keeps the three node defs DRY and the image builds once. Nodes depend_on postgres service_healthy (pg_isready) and have a /healthz busybox-wget healthcheck.

Verified end to end: docker compose up --build brings all three nodes to healthy against the shared DB from an empty schema (concurrent advisory-lock auto-migrate), and a message published to node1 is readable via /history from node2 and node3. README gained a "Local cluster (Docker Compose)" section.
<!-- SECTION:NOTES:END -->
