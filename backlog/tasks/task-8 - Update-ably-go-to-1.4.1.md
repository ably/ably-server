---
id: TASK-8
title: Update ably-go to 1.4.1
status: To Do
assignee: []
created_date: '2026-05-31 16:05'
updated_date: '2026-06-03 13:06'
labels:
  - config
dependencies: []
ordinal: 8000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Bump github.com/ably/ably-go from v1.4.0 to v1.4.1 in go.mod (go get + go mod tidy). The protocol types optionally lean on ably-go's proto package (DESIGN.md §5); verify nothing in internal/protocol regresses and the ably-go integration tests still pass.
<!-- SECTION:DESCRIPTION:END -->
