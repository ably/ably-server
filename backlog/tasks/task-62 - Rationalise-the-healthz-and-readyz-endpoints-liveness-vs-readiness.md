---
id: TASK-62
title: Rationalise the /healthz and /readyz endpoints (liveness vs readiness)
status: Done
assignee:
  - '@claude'
created_date: '2026-06-16 18:23'
updated_date: '2026-07-09 11:34'
labels: []
dependencies: []
ordinal: 62000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Today /healthz and /readyz are identical: both are registered in cmd/ably-server/main.go and both just return 200 "ok" (internal/rest/server.go HandleHealthz/HandleReadyz). That defeats the point of having two — and DESIGN.md (deployment-modes / endpoints table) says /readyz should be a *readiness* check that pings the DB in cluster mode, which it does not currently do.

Decide and implement the intended split:
- /healthz = liveness: process is up and serving; stays cheap and dependency-free (always 200 once the HTTP server is listening).
- /readyz = readiness: ready to take traffic; in cluster mode this should ping Postgres and return 503 when the DB is unreachable, so orchestrators (and the docker-compose stack from TASK-60) don't route to a node that can't serve.

If on reflection we only want one endpoint, document that decision instead and drop the other. Either way the two should stop being silent duplicates, and the docker-compose healthcheck (currently on /healthz) should point at whichever endpoint expresses "ready to serve".
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 /healthz and /readyz are no longer identical: their behaviour matches their documented liveness vs readiness purpose (or one is intentionally removed and that's documented)
- [x] #2 In cluster mode /readyz reflects DB reachability (e.g. 503 when Postgres is unreachable)
- [x] #3 DESIGN.md and the docker-compose healthcheck are updated to match the resulting behaviour
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Split /healthz (liveness, always 200, dependency-free) from /readyz (readiness): in memory/disk modes always 200; in cluster mode ping Postgres and return 503 when unreachable. Plumb a small Pinger interface from internal/storage/postgres into rest.Server: add storage.Pinger (optional interface, Ping(ctx) error) implemented by postgres.Storage (delegates to pool.Ping); rest.NewServer keeps accepting the existing args but HandleReadyz needs the readiness check — add an optional readiness dependency via a new constructor param or a With-option to avoid a breaking signature change beyond what's needed, checking with storage.Storage's optional Pinger via a type assertion at wiring time in main.go. Keep HandleHealthz trivial (200 'ok', no auth, no dependency calls). Update DESIGN.md if wording needs adjusting (already documents the intended behaviour) and docker-compose.yml healthcheck to point at /readyz (readiness = ready to serve, matches TASK-60 intent of not routing to a node that can't reach Postgres). Add unit tests: /healthz always 200; /readyz 200 in memory mode; /readyz 503 when a fake Pinger returns an error; /readyz 200 when Pinger succeeds.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Added storage.Pinger interface (internal/storage/storage.go); postgres.Storage.Ping delegates to pool.Ping and satisfies it. rest.NewServer takes a new storage.Pinger param (nil for memory/disk); HandleHealthz stays a bare dependency-free 200; HandleReadyz pings it with a 2s timeout and returns 503 on error. main.go wires it via a type assertion on the already-constructed storage.Storage. docker-compose healthcheck now polls /readyz.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Split /healthz and /readyz: HandleHealthz stays a bare, dependency-free 200 'ok' liveness probe. HandleReadyz now consults an optional storage.Pinger (new interface in internal/storage) — nil in memory/disk mode (always 200), and backed by postgres.Storage.Ping (pool.Ping) in cluster mode, returning 503 'not ready' within a 2s timeout when Postgres is unreachable. rest.NewServer gained a ready storage.Pinger parameter; main.go wires it via a type assertion on the constructed storage.Storage. Updated DESIGN.md's REST endpoint table to spell out the liveness/readiness split and the docker-compose healthcheck to poll /readyz. Added unit tests for healthz always-200, readyz-reachable, and readyz-unreachable (fake Pinger) plus confirmed /healthz is unaffected by a failing Pinger.
<!-- SECTION:FINAL_SUMMARY:END -->
