---
id: TASK-91
title: >-
  TestRestClient stalls again despite the POST /stats stub — make it terminate
  promptly
status: To Do
assignee: []
created_date: '2026-07-10 14:31'
labels:
  - rest
  - compat
dependencies: []
priority: medium
ordinal: 91000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
TASK-82 (2026-07-09) added POST /stats precisely to stop this hang, and its closing verification recorded the test moving to a clean ~10s FAIL. COMPAT_REPORT_2026-07-10.md reports it hanging to the per-test timeout again — surprising. Verified at HEAD: GET /stats returns 200 with an empty array and POST /stats answers promptly, so the server appears to respond correctly; the suspicion is the SDK's stats retrieval/poll loop retrying empty pages until the harness timeout (the test polls for stats data that a non-collecting server will never produce). Re-run verbosely, identify exactly where the time goes (server never answering something vs an SDK poll loop over well-formed empty responses), fix any server-side stall, and if the residual is purely the SDK polling a documented non-goal, record that with evidence and note the expected steady-state result (fast FAIL vs unavoidable timeout) so future reports classify it correctly.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The stall is traced with evidence (server request/response log for the test run)
- [ ] #2 Any server-side slow/unanswered request is fixed; otherwise the SDK-side poll-loop explanation is recorded in the task and the report expectation documented
<!-- AC:END -->
