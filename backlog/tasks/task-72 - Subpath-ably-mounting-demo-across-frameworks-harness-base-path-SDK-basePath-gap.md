---
id: TASK-72
title: >-
  Subpath (/ably) mounting demo across frameworks + harness --base-path + SDK
  basePath gap
status: To Do
assignee: []
created_date: '2026-06-30 20:43'
labels:
  - embedding-poc
dependencies: []
references:
  - EMBEDDING-POC.md
ordinal: 72000
---

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 host apps mount Ably under /ably alongside their own routes (Go/Node/.NET/Python)
- [ ] #2 harness gains --base-path and proves the server+proxy serve /ably/* at the wire level
- [ ] #3 documents the SDK basePath change needed for stock SDKs to target a subpath (honest: not possible today)
<!-- AC:END -->
