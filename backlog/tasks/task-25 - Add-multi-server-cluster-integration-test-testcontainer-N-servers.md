---
id: TASK-25
title: Add multi-server cluster integration test (testcontainer + N servers)
status: To Do
assignee: []
created_date: '2026-05-31 16:27'
updated_date: '2026-05-31 16:28'
labels: []
dependencies:
  - TASK-22
  - TASK-23
ordinal: 25000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Add an integration test (same `integration` build tag) that brings up one PostgreSQL testcontainer plus multiple ably-server instances in cluster mode, then verifies cross-node pub/sub: a publish sent through each server reaches subscribers attached to every other server (full mesh), with correct ordering and no duplicates. This exercises the LISTEN/NOTIFY broker end to end across nodes. Depends on the cluster pub/sub broker and auto-migrate.
<!-- SECTION:DESCRIPTION:END -->
