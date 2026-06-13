---
id: TASK-3
title: Remove the "At this stage" comment from internal/rest/server.go
status: To Do
assignee: []
created_date: '2026-05-31 16:05'
updated_date: '2026-06-03 13:06'
labels:
  - cleanup
dependencies: []
ordinal: 3000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The rest package doc comment (internal/rest/server.go:1-5) carries a transitional "At this stage the endpoints are ... History is not yet implemented ..." note. Remove the "At this stage ..." paragraph (lines 3-5), leaving the stable one-line package summary. Mirrors TASK-1, which did the same for cmd/ably-server/main.go. The history endpoint itself is a separate task; this is just the comment cleanup.
<!-- SECTION:DESCRIPTION:END -->
