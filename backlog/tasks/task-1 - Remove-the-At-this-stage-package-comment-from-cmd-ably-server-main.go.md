---
id: TASK-1
title: Remove the "At this stage" package comment from cmd/ably-server/main.go
status: To Do
assignee: []
created_date: '2026-05-31 14:16'
updated_date: '2026-06-03 13:06'
labels:
  - cleanup
dependencies: []
ordinal: 1000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The main package doc comment (cmd/ably-server/main.go:1-4) carries a transitional "At this stage it terminates WebSocket connections at `/`, sends a CONNECTED frame..." note describing the server's early behaviour. This snapshot will drift as the server grows. Remove the "At this stage..." sentence (lines 3-4), leaving the stable one-line summary on line 1.
<!-- SECTION:DESCRIPTION:END -->
