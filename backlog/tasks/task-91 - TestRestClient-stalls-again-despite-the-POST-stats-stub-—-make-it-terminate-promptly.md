---
id: TASK-91
title: >-
  TestRestClient stalls again despite the POST /stats stub — make it terminate
  promptly
status: Done
assignee:
  - '@claude'
created_date: '2026-07-10 14:31'
updated_date: '2026-07-11 23:52'
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
- [x] #1 The stall is traced with evidence (server request/response log for the test run)
- [x] #2 Any server-side slow/unanswered request is fixed; otherwise the SDK-side poll-loop explanation is recorded in the task and the report expectation documented
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Boot local server with --fixtures, enable request logging (slog + ably_http_requests_total). 2. Run TestRestClient verbosely against it, capture server-side request log + client timing. 3. Determine whether any request never gets a response (server stall) vs SDK poll-loop over well-formed empty stats pages. 4. Fix any genuine server stall; if residual is SDK polling a documented non-goal (stats), record evidence + expected steady-state classification and close honestly.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
TRACED (HEAD abfc8d0; report commit c6f4001 is 2 backlog-only commits behind, so same server code).

Ran TestRestClient verbosely against a locally-booted server (--mode memory --fixtures ... --log-level debug):
  --- FAIL: TestRestClient (10.01s)
      PASS encoding_messages/{json,msgpack}, PASS Time
      FAIL Stats (10.00s): 'timeout waiting for client.Stats to return nonempty value'
Clean ~10s FAIL — NOT a hang to the per-test timeout. Not reproducible as a 60s stall at this code.

Server-side request tally (ably_http_requests_total after the run):
  GET  /stats  -> 20x status=200   (the test's 500ms tick over its own 10s deadline)
  POST /stats  -> 1x  status=201   (prompt no-op)
  GET  /time   -> 2x  status=200
Every request received a prompt response; no unanswered/slow request server-side.

ROOT CAUSE: purely client/test-side. The Stats subtest (ably/rest_client_integration_test.go:122-205) POSTs stats then polls client.Stats() every 500ms under its own 'timeout := time.After(time.Second * 10)'. Statistics collection is a DESIGN.md §1 non-goal: GET /stats always returns 200 []. The poll can never observe a nonempty page, so after 10s the goroutine sends errors.New('timeout waiting...') and t.Fatal fails the test. Same class as TASK-90: an unavoidable consequence of the stats non-goal, not a server defect.

EXPECTED STEADY STATE for future reports: fast ~10s FAIL (not a hang/PANIC). The 07-10 report's 'hangs to timeout' classification is not reproducible; it should be recorded as an expected fast FAIL against the stats non-goal. No server fix warranted.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Traced with a locally-booted server + verbose test run + server-side metrics. TestRestClient does a clean ~10s FAIL (not a hang): 20x GET /stats->200, 1x POST /stats->201, all prompt, no unanswered request. The residual 10s is the Stats subtest's own time.After(10s) polling client.Stats() for a nonempty page that a non-collecting server (DESIGN §1 stats non-goal) never produces. No server-side stall exists, so no code change; recorded the SDK-poll-loop explanation and the expected steady-state classification (fast FAIL, not timeout) for future reports.
<!-- SECTION:FINAL_SUMMARY:END -->
