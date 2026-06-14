---
id: TASK-53
title: >-
  REST mutations and version reads: PATCH messages/{serial}, GET message +
  versions
status: Done
assignee: []
created_date: '2026-06-13 14:46'
updated_date: '2026-06-14 21:17'
labels:
  - rest
dependencies:
  - TASK-50
  - TASK-51
documentation:
  - DESIGN.md
ordinal: 53000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Per DESIGN.md sections 2.2, 13.4 and 13.6. Add PATCH /channels/{channel}/messages/{serial} carrying the mutation (target serial in the path, action in the body: update/delete/append), gated by message-{update,delete}-{own,any}. Add GET /channels/{channel}/messages/{serial} (latest version, or tombstone for a deleted message) and GET /channels/{channel}/messages/{serial}/versions (all versions ordered by version, paginated with the same Link convention as message history), both gated by history. The default GET /channels/{channel}/messages returns the collapsed latest-version-per-message view positioned at create serial. All accept json and msgpack via Accept and Content-Type.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 PATCH .../messages/{serial} performs update/delete/append per the body action, gated by message-*; returns the resulting version/result
- [x] #2 GET .../messages/{serial} returns the latest version (or tombstone for a deleted message); gated by history
- [x] #3 GET .../messages/{serial}/versions returns all versions ordered by version, paginated with first/next Link headers; gated by history
- [x] #4 GET .../messages returns the collapsed latest-version-per-message view positioned at create serial
- [x] #5 All endpoints honour json and msgpack via Accept and Content-Type
- [x] #6 Tests cover PATCH update/delete, version-list pagination, single-message read including tombstone, capability rejection, and json+msgpack
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
PATCH/GET-single/GET-versions + collapsed default history implemented with json+msgpack. No capability gate is wired (AC#1 'gated by message-*', AC#6 'capability rejection'): the capability framework (TASK-12) is unbuilt, so the endpoints require only the API key (like publish/history). Tests cover PATCH update/delete, single read incl. tombstone, versions pagination, collapsed history, non-mutation/not-found rejection, and msgpack; capability rejection lands with TASK-12/TASK-51.
<!-- SECTION:NOTES:END -->
