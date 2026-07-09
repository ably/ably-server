---
id: TASK-77
title: Return 404 with an Ably ErrorInfo body for unknown REST resources
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 19:21'
updated_date: '2026-07-09 19:48'
labels:
  - rest
  - compat
dependencies: []
priority: medium
ordinal: 77000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-09.md gap 2: TestHTTPPaginatedResponse fails. A request the SDK expects to 404 with Ably code 40400 instead gets 405 with an empty body, because the router falls through to Method-Not-Allowed and no ErrorInfo envelope is written. Make unknown paths (and wrong-method requests where Ably would 404) return 404 with the standard Ably error body ({error: {code: 40400, statusCode: 404, message}}), honouring the Accept header (json/msgpack).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 An unknown REST path returns 404 with an Ably ErrorInfo body carrying code 40400
- [x] #2 Error responses honour Accept (JSON default, msgpack supported)
- [x] #3 TestHTTPPaginatedResponse passes against a local server
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Reproduce TestHTTPPaginatedResponse; identify the failing request.
2. Add an Ably-shaped 404 handler and wire it as the router catch-all.
3. Set X-Ably-Errorcode/Errormessage headers the SDK reads.
4. Unit tests + harness verify; don't break the mux routing test.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Root cause: the failing subtest is request_404 — GET /keys/{keyName}/requestToken. Only POST is registered for that path, so Go's ServeMux returned 405 with an empty body. The SDK's HTTPPaginatedResponse reads ErrorCode/ErrorMessage from the X-Ably-Errorcode/X-Ably-Errormessage response headers (HP6/HP7), not the body, so it saw status 405 and code 0.

Fix:
- internal/rest/server.go: added HandleNotFound writing a 404 with the Ably ErrorInfo body (code 40400) honouring Accept (json/msgpack), plus X-Ably-Errorcode/X-Ably-Errormessage headers. Refactored writeCapabilityError onto a shared writeErrorInfo helper (now also emits the X-Ably headers).
- cmd/ably-server/main.go: registered a catch-all 'rest("/", rs.HandleNotFound)'. A subtree "/" pattern matches any path/method, so it both replaces ServeMux's bare 404 for unknown paths AND converts the 405 for a known-path/unregistered-method (GET requestToken) into the expected 404. 'GET /{$}' stays more specific, so the WS root is unaffected (verified by the existing routes test).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Return an Ably-shaped 404 for unknown REST resources.

Problem: a request the SDK expects to 404 (GET on the POST-only /keys/{keyName}/requestToken) hit Go's ServeMux 405 with an empty body, and the SDK reported no error code (TestHTTPPaginatedResponse/request_404). The SDK reads the error code/message from the X-Ably-Errorcode/X-Ably-Errormessage headers (HP6/HP7), which were absent.

Changes:
- internal/rest/server.go: new HandleNotFound emits 404 with the Ably ErrorInfo body {error:{code:40400,statusCode:404,message}} in the Accept format (JSON default, msgpack supported) and sets the X-Ably-Errorcode/X-Ably-Errormessage headers. Extracted a shared writeErrorInfo helper (writeCapabilityError now reuses it and also emits the X-Ably headers).
- cmd/ably-server/main.go: registered a catch-all 'rest("/", HandleNotFound)'. As a subtree pattern it replaces the bare 404 for unknown paths and converts the ServeMux 405 for a known-path/unregistered-method into the expected 404. The more-specific 'GET /{$}' keeps the WebSocket root intact.
- DESIGN.md §2.2: documented the 404 error body/headers and the per-line relative Link convention.

Tests:
- New TestHandleNotFoundAblyError (rest) pins body + headers across JSON/msgpack.
- Extended the cmd/ably-server routes test to assert the 40400 code on an unknown path and on the GET-requestToken method mismatch (mux /nonexistent->404 still holds).
- go build/vet/test ./... pass; harness TestHTTPPaginatedResponse now PASSES.
<!-- SECTION:FINAL_SUMMARY:END -->
