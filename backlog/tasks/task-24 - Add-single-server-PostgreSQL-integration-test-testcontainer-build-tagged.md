---
id: TASK-24
title: 'Add single-server PostgreSQL integration test (testcontainer, build-tagged)'
status: To Do
assignee: []
created_date: '2026-05-31 16:27'
updated_date: '2026-05-31 16:28'
labels: []
dependencies:
  - TASK-21
  - TASK-23
ordinal: 24000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Add an integration test that runs the server against a real PostgreSQL spun up via testcontainers-go, covering the Postgres storage path end to end (publish -> persist -> history / channelSerial replay). Gate it behind an explicit build tag (e.g. //go:build integration) so plain `go test ./...` runs only unit tests by default, and integration tests run via `go test -tags=integration ./...`. The test relies on auto-migrate to provision the schema. Adds the testcontainers-go dependency. Depends on the Postgres storage implementation and auto-migrate.
<!-- SECTION:DESCRIPTION:END -->
