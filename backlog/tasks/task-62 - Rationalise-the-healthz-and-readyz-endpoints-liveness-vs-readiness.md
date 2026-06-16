---
id: TASK-62
title: Rationalise the /healthz and /readyz endpoints (liveness vs readiness)
status: To Do
assignee: []
created_date: '2026-06-16 18:23'
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
- [ ] #1 /healthz and /readyz are no longer identical: their behaviour matches their documented liveness vs readiness purpose (or one is intentionally removed and that's documented)
- [ ] #2 In cluster mode /readyz reflects DB reachability (e.g. 503 when Postgres is unreachable)
- [ ] #3 DESIGN.md and the docker-compose healthcheck are updated to match the resulting behaviour
<!-- AC:END -->
