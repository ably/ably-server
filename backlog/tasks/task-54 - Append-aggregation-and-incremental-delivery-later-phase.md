---
id: TASK-54
title: Append aggregation and incremental delivery (streamed appends)
status: To Do
assignee: []
created_date: '2026-06-13 14:46'
updated_date: '2026-07-09 11:07'
labels:
  - mutable-messages
dependencies:
  - TASK-50
  - TASK-52
  - TASK-53
documentation:
  - DESIGN.md
priority: high
ordinal: 54000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Per DESIGN.md section 13.3. Implement append semantics on top of the core update/delete path. An append concatenates its data onto the message's current latest version (name and extras follow the same shallow-mixin replace as update). The server maintains the rolled-up latest data: a caught-up subscriber receives each append incrementally (just the delta data), while the first delivery for a message a subscriber has not yet seen — e.g. immediately after attach — is a full action=update with the aggregated payload, after which it receives subsequent appends incrementally. The server may conflate: coalesce multiple appends, drop superseded intermediate versions, or deliver an append as a full rolled-up update; the only guarantee is that the last version a subscriber receives is the most recent. Appends are not retained as individual entries in version history (only the aggregated latest is durable). A channel param lets a subscriber opt into full versions instead of incremental appends. This is explicitly a later phase, after the core update/delete path ships.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 append concatenates data onto the latest version; name and extras follow shallow-mixin replace
- [ ] #2 A caught-up subscriber receives incremental appends; the first delivery for a not-yet-seen message is a full action=update with the aggregated payload
- [ ] #3 The server may conflate appends and intermediate versions; the last delivered version is guaranteed to be the most recent
- [ ] #4 Appends are not retained as individual entries in version history; the aggregated latest version is durable
- [ ] #5 A channel param lets a subscriber opt into full versions instead of incremental appends
- [ ] #6 Tests cover incremental delivery, full-on-attach aggregation, conflation, and the full-versions channel param
<!-- AC:END -->
