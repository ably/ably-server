---
id: TASK-65
title: >-
  M2: Node embedded-server package: binary resolve+supervise + Express/Fastify
  WS proxy
status: To Do
assignee: []
created_date: '2026-06-30 16:31'
labels:
  - embedding-poc
dependencies: []
references:
  - EMBEDDING-POC.md
ordinal: 65000
---

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 spawns binary on a free port, health-checks /readyz, restarts on crash, shuts down with app
- [ ] #2 Express and Fastify middleware proxy realtime traffic incl. WebSocket
- [ ] #3 harness passes through an example Node app; metrics recorded
<!-- AC:END -->
