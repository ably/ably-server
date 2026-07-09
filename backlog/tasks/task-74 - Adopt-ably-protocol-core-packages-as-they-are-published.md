---
id: TASK-74
title: Adopt ably/protocol-core packages as they are published
status: To Do
assignee: []
created_date: '2026-07-09 11:07'
labels: []
dependencies: []
documentation:
  - >-
    https://ably.atlassian.net/wiki/spaces/~5ef9be47c796b50bb2e1ae5c/pages/5226397709
priority: low
ordinal: 74000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The shared-protocol-core RFC makes ably-server a consumer of github.com/ably/protocol-core, a read-only mirror of protocol-visible code extracted from the realtime monorepo (tier 1 first: capabilities, token/auth semantics, wire types; error codes). When the mirror exists, require it in go.mod, swap the extracted packages in for the local internal equivalents (internal/protocol wire types, capabilities matching, token semantics), delete the superseded code, and set up Renovate to open bump PRs gated by this repo's tests. Blocked on the realtime-side extraction landing — track the RFC.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 go.mod requires github.com/ably/protocol-core once published
- [ ] #2 Local implementations superseded by extracted packages are deleted, not kept in parallel
- [ ] #3 Renovate (or equivalent) opens automatic bump PRs gated by tests
<!-- AC:END -->
