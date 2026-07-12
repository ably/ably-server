---
id: TASK-109
title: 'Mutation REST responses: operation/version metadata wire shape'
status: To Do
assignee: []
created_date: '2026-07-12 13:59'
labels:
  - compat
  - ably-js
dependencies: []
priority: medium
ordinal: 109000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
rest/updates-deletes has 3 failures where update/delete/append WITH operation metadata (clientId/description/metadata on the operation) come back missing fields: 'Should update a message (with operation metadata)' and append fail 'AssertionError: the argument to above must be a number' (a numeric field the SDK asserts on — likely version.timestamp — is absent) and 'Should delete a message (with operation metadata)' fails 'expected undefined to deeply equal {}' (operation/metadata object missing from the read-back message). The basic mutation flows pass (10/13), so this is the response/projection shape for the operation envelope: persist the operation details (clientId, description, metadata) with the version and surface them in PATCH/DELETE responses, GET message, and history reads. Compare with ably-js's expected Message.version/Message.operation shape (TM2p/TM2o). The realtime side of updates-deletes is blocked by TASK-97 — re-run it after that fix to see what residual gaps remain.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Mutations with operation metadata round-trip: version carries timestamp etc., operation carries clientId/description/metadata
- [ ] #2 The three failing rest/updates-deletes tests pass
<!-- AC:END -->
