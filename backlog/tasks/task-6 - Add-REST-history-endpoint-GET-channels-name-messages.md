---
id: TASK-6
title: 'Add REST history endpoint (GET /channels/{name}/messages)'
status: To Do
assignee: []
created_date: '2026-05-31 16:05'
labels: []
dependencies: []
ordinal: 6000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Implement GET /channels/{channel}/messages (DESIGN.md §2.2) returning paginated channel history, backed by storage.ChannelStore.History (already defined in internal/storage/storage.go). Requires the `history` capability (§3.1). Paginate via Ably's Link header convention (first, next) per §2.2, and support the usual direction/limit query params consistent with Ably. Implement in internal/rest and register on the mux in cmd/ably-server/main.go alongside the existing POST publish handler. Remove the "History is not yet implemented" note from the package doc once done.
<!-- SECTION:DESCRIPTION:END -->
