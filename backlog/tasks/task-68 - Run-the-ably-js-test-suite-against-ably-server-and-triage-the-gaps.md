---
id: TASK-68
title: Run the ably-js test suite against ably-server and triage the gaps
status: To Do
assignee: []
created_date: '2026-07-09 11:06'
labels: []
dependencies: []
documentation:
  - 'https://ably.atlassian.net/wiki/spaces/product/pages/5171281935'
priority: high
ordinal: 68000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PDR-090's experimental release explicitly requires 'full client SDK (eg ably-js) test coverage (for the functional scope)'. Only ably-go has been exercised so far (COMPAT_REPORT.md). Stand up a harness that points ably-js's realtime/REST test suite at a local ably-server (endpoint/port/TLS overrides, sandbox app-provisioning bypassed), run the suite, and produce a compatibility report mapping each failure to an existing backlog task, a new task, or an explicit non-goal — following the file-tasks-for-incompatibilities convention.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 ably-js test suite runs against a local ably-server via a committed, documented harness
- [ ] #2 A compatibility report maps every failure to a backlog task or documented non-goal
- [ ] #3 New tasks filed for in-scope gaps the run uncovers
<!-- AC:END -->
