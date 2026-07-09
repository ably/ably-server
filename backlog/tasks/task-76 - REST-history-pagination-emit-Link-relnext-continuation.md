---
id: TASK-76
title: 'REST history pagination: emit Link rel=next continuation'
status: To Do
assignee: []
created_date: '2026-07-09 19:21'
labels:
  - rest
  - compat
dependencies: []
priority: high
ordinal: 76000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-09.md gap 1: TestHistory_RSL2_RSL2b3 fails. GET /channels/{name}/messages honours limit for the first page but returns no continuation link, so the SDK cannot page (expected all 10 messages, got the first page). DESIGN.md §2.2 already specifies Ably's Link header convention (first, next) — implement it on the history read paths (message history, presence history, message versions) using the last-returned channelSerial as the cursor, omitting next on the final page.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 GET message history returns a Link rel=next header whenever more results exist, and the SDK can walk pages to exhaustion
- [ ] #2 Presence history and message-version reads follow the same convention
- [ ] #3 No next link is emitted on the final page
- [ ] #4 TestHistory_RSL2_RSL2b3 passes against a local server
<!-- AC:END -->
