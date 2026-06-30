---
id: TASK-72
title: >-
  Subpath (/ably) mounting demo across frameworks + harness --base-path + SDK
  basePath gap
status: Done
assignee:
  - '@claude'
created_date: '2026-06-30 20:43'
updated_date: '2026-06-30 21:36'
labels:
  - embedding-poc
dependencies: []
references:
  - EMBEDDING-POC.md
ordinal: 72000
---

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 host apps mount Ably under /ably alongside their own routes (Go/Node/.NET/Python)
- [x] #2 harness gains --base-path and proves the server+proxy serve /ably/* at the wire level
- [x] #3 documents the SDK basePath change needed for stock SDKs to target a subpath (honest: not possible today)
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Subpath (/ably) mounting across Go (StripPrefix), Node Express (pathFilter+pathRewrite), Node Fastify (prefix+rewritePrefix), .NET (YARP PathRemovePrefix), Python (ASGI fall-through). Harness gained --base-path + a raw-protocol rawpubsub scenario; proves the server+proxy serve /ably/* (rawpubsub+resume PASS) while a bare /readyz is 404 and the host keeps /. Documented the SDK basePath gap (stock SDKs can't target a subpath yet — raw clients/harness can).
<!-- SECTION:FINAL_SUMMARY:END -->
