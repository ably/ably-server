---
id: TASK-86
title: >-
  Triage the untriaged panics: TestAuth_IgnoreTimestamp_QueryTime and
  RTL6c2_PublishEnqueue
status: To Do
assignee: []
created_date: '2026-07-09 19:22'
labels:
  - compat
dependencies: []
priority: low
ordinal: 86000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-09.md gap 8: both panic with the nil-deref-against-unimplemented pattern and were not individually triaged this run. TestAuth_IgnoreTimestamp_QueryTime exercises authWithQueryTime (token requests timestamped from GET /time — implemented, so the panic needs explaining); RTL6c2_PublishEnqueue exercises publish-while-connecting queueing (mostly SDK-side). Re-run each in verbose mode, identify the missing/misshapen server response, file or fix accordingly.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Both tests have a recorded root cause
- [ ] #2 Server-side gaps are fixed or spun into their own tasks; harness artifacts documented
<!-- AC:END -->
