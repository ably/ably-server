---
id: TASK-39
title: 'Support --log-format {text|json}'
status: Done
assignee:
  - '@claude'
created_date: '2026-06-13 08:41'
updated_date: '2026-07-09 11:27'
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
- [x] #1 --log-format flag exists with an ABLY_SERVER_* env equivalent, accepting text or json, defaulting to text
- [x] #2 json selects a JSON slog handler; text selects the text handler
- [x] #3 An unrecognised value is a startup error
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Add --log-format flag (env ABLY_SERVER_LOG_FORMAT) accepting text|json, default text. Rework newLogger in cmd/ably-server/main.go to take format and return (*slog.Logger, error) so an unrecognised value is a startup error surfaced via run(). Select slog.NewTextHandler vs slog.NewJSONHandler. Add unit tests in main_test.go / a logger test for text/json selection and invalid-value error. Update DESIGN.md if needed (already documents the flag). Commit as one commit for TASK-39.
<!-- SECTION:PLAN:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Added --log-format {text|json} flag (env ABLY_SERVER_LOG_FORMAT, default text). newLogger now takes a format string and returns (*slog.Logger, error); an unrecognised value is surfaced as a startup error via run(). Added unit tests covering flag/env selection and JSON vs text handler output.
<!-- SECTION:FINAL_SUMMARY:END -->
