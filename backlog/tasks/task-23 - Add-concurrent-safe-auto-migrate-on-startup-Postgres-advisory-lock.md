---
id: TASK-23
title: Add concurrent-safe auto-migrate on startup (Postgres advisory lock)
status: To Do
assignee: []
created_date: '2026-05-31 16:27'
updated_date: '2026-05-31 16:28'
labels: []
dependencies:
  - TASK-21
ordinal: 23000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
On startup every server process should be able to apply pending schema migrations, but only one runs them at a time. Acquire a Postgres advisory lock (pg_advisory_lock on a fixed key) so a single process holds the migration lock, applies migrations, then releases; other processes block until it finishes and then observe an up-to-date schema (effective no-op). Track applied migrations (e.g. a schema_migrations table) and ship migrations as embedded SQL. Must be safe for a cold start where N nodes boot simultaneously against an empty database. Migration mechanism (hand-rolled vs a library such as golang-migrate) is open; the advisory-lock serialisation is the key requirement. Depends on the Postgres storage implementation/schema.
<!-- SECTION:DESCRIPTION:END -->
