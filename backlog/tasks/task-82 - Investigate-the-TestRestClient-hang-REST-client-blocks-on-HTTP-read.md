---
id: TASK-82
title: Investigate the TestRestClient hang (REST client blocks on HTTP read)
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 19:22'
updated_date: '2026-07-09 20:21'
labels:
  - rest
  - compat
dependencies: []
priority: medium
ordinal: 82000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-09.md gap 4: TestRestClient times out blocked on an HTTP read; the report suspects the /stats call stalling rather than failing fast. The stats stub endpoint (see the stats task) may resolve it — after that lands, re-run; if the hang persists, trace which request blocks (server never writing a response vs SDK retry loop) and fix the server side to always answer promptly.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 The blocking request is identified
- [x] #2 TestRestClient no longer hangs against a local server
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Re-run TestRestClient after the /stats stub.
2. If still hanging, trace which request blocks (server vs SDK).
3. Fix server-side to answer promptly; add a unit test.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Root cause identified via pprof + a logging proxy + a standalone reproducer:

- The blocking request is POST /stats. ably-go's TestRestClient/Stats does client.Post(ctx, "/stats", &stats, nil) to WRITE stats before polling them back. The server only served GET /stats, so POST /stats fell through to the catch-all 404 (TASK-77).
- The SDK's REST write path (c.do -> handleResponse -> checkValidHTTPResponse) treats a non-2xx as an error and calls io.ReadAll on the response body to decode the ErrorInfo. Combined with the request carrying an unread msgpack body, the SDK blocked reading that error body indefinitely (the client's persistConn readLoop sat in IO wait). The client's overall timeout is 60s, exceeding the test's timeout, so it presented as a hang.
- Server-side evidence it was NOT a slow/missing server response: a pprof goroutine dump of ably-server during the hang showed NO active REST handlers (server idle); curl and a standalone Go client (raw + public SDK API) always got prompt, well-formed responses (correct Content-Length) for GET/POST /stats in both JSON and msgpack.

Fix: serve POST /stats as a no-op stats-injection stub (HandlePostStats): authenticate like GET /stats (app-wide stats op), drain the request body, return an empty 201. This mirrors the real Ably sandbox accepting stat writes and lets the SDK's write succeed so it returns promptly.

Result: TestRestClient no longer hangs. It now FAILS cleanly at ~10s on the test's own 'timeout waiting for client.Stats to return nonempty value' — expected, because statistics collection is a documented non-goal (DESIGN §1: GET /stats always returns an empty array), so the poll can never observe non-empty stats. Harness goes from PANIC (hang) to FAIL (clean).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Stop TestRestClient hanging by serving POST /stats as a no-op stub.

Blocking request identified: POST /stats. ably-go's TestRestClient/Stats writes stats via client.Post before polling them back. The server only served GET /stats, so POST /stats hit the catch-all 404. The SDK's REST write path treats a non-2xx as an error and reads the response body to decode the ErrorInfo; with the request carrying an unread body, the SDK blocked reading that error body (its 60s client timeout exceeds the test timeout, presenting as a hang). A pprof dump showed ably-server idle during the hang (no stuck handlers) and direct HTTP/SDK probes always got prompt, well-formed responses — confirming the server already answers promptly and the block was on the SDK's side of a 404 it didn't expect.

Change:
- internal/rest/server.go: HandlePostStats — authenticates like GET /stats (app-wide stats op), drains the request body, returns an empty 201. Statistics remain uncollected (non-goal).
- cmd/ably-server/main.go: registered POST /stats.
- DESIGN.md §1 and the §2.2 table document the no-op.

Result: the hang is gone. TestRestClient now FAILS cleanly at ~10s on the test's own 'timeout waiting for client.Stats to return nonempty value', because statistics collection is a documented non-goal so GET /stats always returns empty — the poll can never see non-empty stats. Harness classification moves from PANIC (hang) to FAIL (clean); AC 'no longer hangs' is satisfied.

Tests:
- New TestPostStatsIsPromptNoOp (rest) asserts POST /stats answers promptly with 201.
- go build/vet/test ./... pass; harness TestRestClient no longer hangs (FAIL, not PANIC).
<!-- SECTION:FINAL_SUMMARY:END -->
