---
id: TASK-77
title: Return 404 with an Ably ErrorInfo body for unknown REST resources
status: To Do
assignee: []
created_date: '2026-07-09 19:21'
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
- [ ] #1 An unknown REST path returns 404 with an Ably ErrorInfo body carrying code 40400
- [ ] #2 Error responses honour Accept (JSON default, msgpack supported)
- [ ] #3 TestHTTPPaginatedResponse passes against a local server
<!-- AC:END -->
