---
id: TASK-50
title: >-
  Storage: message version log, latest-version messages projection, version
  reads
status: To Do
assignee: []
created_date: '2026-06-13 14:45'
labels:
  - storage
dependencies:
  - TASK-49
  - TASK-43
documentation:
  - DESIGN.md
ordinal: 50000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Per DESIGN.md sections 6, 6.2, 6.3 and 13.1-13.4. Building on the channel_messages log and projection pattern from TASK-43, make Store handle update/delete/append: validate the target serial exists within retention, apply shallow-mixin merge against the message's current latest version (only supplied data/name/extras replace; the rest carried forward), mint a fresh version, persist the merged version cm on channel_messages with message_serial set, and maintain two derived structures atomically — a serial-to-versions secondary index over the log (partial index in postgres / versions bucket in bbolt / map in memory) and the materialised messages projection (latest merged version per serial, with a deleted tombstone). A delete is soft: the message and all its versions remain queryable. History gains two message modes: the default collapses to the latest version of each message positioned at its create serial; a by-serial scan returns every version ordered by version. Add a single-message latest read. Implement across memory, bbolt and postgres behind the shared contract suite.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 channel_messages gains a message_serial column (NULL for presence) with a partial serial-to-versions index; bbolt gains versions and messages buckets; memory uses maps
- [ ] #2 The materialised messages projection holds the latest merged version per message serial with a deleted tombstone; created via forward-only migration in postgres
- [ ] #3 Store handles update/append/delete: validates the target exists (else error), applies shallow-mixin merge, mints a fresh version, and upserts the projection plus versions index in the same transaction as the log insert
- [ ] #4 A delete is soft: the projection row is marked deleted but the message and all versions remain queryable
- [ ] #5 Default message history collapses to the latest version of each message positioned at its create serial
- [ ] #6 A by-serial version scan returns every version ordered by version; a single-message read returns the latest version (or tombstone)
- [ ] #7 When a message's last surviving version ages out of channel_messages, retention drops its projection row
- [ ] #8 The contract suite covers create-update-delete-versions, shallow-mixin merge, soft-delete visibility, collapsed vs version history, and idempotent mutation; passes on all three backends
<!-- AC:END -->
