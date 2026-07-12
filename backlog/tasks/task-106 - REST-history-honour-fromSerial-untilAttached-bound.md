---
id: TASK-106
title: 'REST history: honour fromSerial (untilAttached) bound'
status: To Do
assignee: []
created_date: '2026-07-12 13:59'
labels:
  - compat
  - ably-js
dependencies: []
priority: medium
ordinal: 106000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
ably-js history({untilAttached:true}) sends fromSerial=<the channel's attachSerial> on GET /channels/{name}/messages, expecting only messages up to the attach point. parseHistoryQuery (internal/rest/server.go:1363) parses only direction/start/end/limit; fromSerial is silently ignored, so all messages come back — realtime/history history_until_attach fails ('expected 5, got 10'; the file's only test). Fix: parse fromSerial, thread it into storage.HistoryQuery as a serial-bound, and apply it in the storage backends (serials are lexicographically ordered so it composes with the existing paging).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 GET history with fromSerial returns only messages bounded by that serial in both directions
- [ ] #2 history_until_attach passes
<!-- AC:END -->
