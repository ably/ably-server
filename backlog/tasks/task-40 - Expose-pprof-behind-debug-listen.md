---
id: TASK-40
title: Expose pprof behind --debug-listen
status: To Do
assignee: []
created_date: '2026-06-13 08:41'
labels:
  - ops
dependencies: []
ordinal: 40000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
DESIGN.md §10 specifies Go pprof served on a separate port behind a --debug-listen flag. Not implemented. Serve net/http/pprof on a dedicated listener when the flag is set.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 --debug-listen flag exists with an ABLY_SERVER_* env equivalent
- [ ] #2 When set, the standard net/http/pprof endpoints are served on that separate address
- [ ] #3 When unset, no debug/pprof listener is started
<!-- AC:END -->
