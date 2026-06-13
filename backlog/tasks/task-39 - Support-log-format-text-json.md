---
id: TASK-39
title: 'Support --log-format {text|json}'
status: To Do
assignee: []
created_date: '2026-06-13 08:41'
labels:
  - ops
dependencies: []
ordinal: 39000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
DESIGN.md §9/§10 specify a --log-format flag selecting slog text output (dev) or json (prod). Only --log-level exists in cmd/ably-server today; the format is fixed. Add --log-format selecting the slog handler.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 --log-format flag exists with an ABLY_SERVER_* env equivalent, accepting text or json, defaulting to text
- [ ] #2 json selects a JSON slog handler; text selects the text handler
- [ ] #3 An unrecognised value is a startup error
<!-- AC:END -->
