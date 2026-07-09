---
id: TASK-79
title: Fix idempotent-publishing tests still failing after TASK-18
status: To Do
assignee: []
created_date: '2026-07-09 19:21'
labels:
  - protocol
  - compat
dependencies:
  - TASK-18
priority: high
ordinal: 79000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-09.md was generated at HEAD, which includes TASK-18 (batch id stamping + storage idempotency), yet TestIdempotentPublishing and TestIdempotent_retry still report duplicate publishes not being de-duplicated. Diagnose against ably-go's idempotent REST publishing (RSL1k: the SDK pre-stamps client-side ids of the form <base>:<idx> and retries the same request): trace where the duplicate slips through — id validation, the idempotency index lookup, or response mapping — and fix it.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 TestIdempotentPublishing passes against a local server
- [ ] #2 TestIdempotent_retry passes against a local server
<!-- AC:END -->
