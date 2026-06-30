---
id: TASK-70
title: >-
  Python embedded-server track (FastAPI/Starlette + a second framework) +
  harness + native smoke
status: To Do
assignee: []
created_date: '2026-06-30 20:43'
labels:
  - embedding-poc
dependencies: []
references:
  - EMBEDDING-POC.md
ordinal: 70000
---

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Python host supervises the ably-server child and reverse-proxies WS+REST on a dedicated port (ASGI)
- [ ] #2 Go conformance harness passes through the Python proxy; fault injection run (kill-9 restart, clean shutdown)
- [ ] #3 native ably (python) SDK does a pub/sub round-trip through the proxy; a second framework covered
<!-- AC:END -->
