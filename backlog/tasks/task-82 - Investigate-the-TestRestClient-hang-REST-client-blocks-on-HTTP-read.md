---
id: TASK-82
title: Investigate the TestRestClient hang (REST client blocks on HTTP read)
status: To Do
assignee: []
created_date: '2026-07-09 19:22'
labels:
  - rest
  - compat
dependencies: []
priority: medium
ordinal: 82000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-09.md gap 4: TestRestClient times out blocked on an HTTP read; the report suspects the /stats call stalling rather than failing fast. The stats stub endpoint (see the stats task) may resolve it — after that lands, re-run; if the hang persists, trace which request blocks (server never writing a response vs SDK retry loop) and fix the server side to always answer promptly.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The blocking request is identified
- [ ] #2 TestRestClient no longer hangs against a local server
<!-- AC:END -->
