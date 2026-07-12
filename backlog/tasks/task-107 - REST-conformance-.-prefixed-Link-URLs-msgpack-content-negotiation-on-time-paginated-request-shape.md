---
id: TASK-107
title: >-
  REST conformance: ./-prefixed Link URLs, msgpack content negotiation on /time,
  paginated request() shape
status: To Do
assignee: []
created_date: '2026-07-12 13:59'
labels:
  - compat
  - ably-js
dependencies: []
priority: medium
ordinal: 107000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Three REST-surface conformance gaps from the 2026-07-12 ably-js run, grouped because they all live in the REST response envelope: (1) Link headers: the server emits Link: <messages?...>; rel=next but ably-js's paginatedresource getRelParams only accepts urls matching ^\./(\w+)\?  — i.e. './messages?...' with an explicit ./ prefix (Ably emits this). Our relative-without-./ links parse as no-next → rest/history history_simple_paginated_b/f, history_multiple_paginated_b/f (5 tests) fail 'Verify next link is present' even though pagination works (verified via curl: next link present, correct from=serial). ably-go tolerated the bare form. (2) /time ignores 'Accept: application/x-msgpack' and returns JSON ('[178386...]'), so the msgpack-format client fails with '14 trailing bytes' (rest/request request_time binary). Audit content negotiation on all REST endpoints while there. (3) rest/request request_post_get_messages dies with 'TypeError: Cannot read properties of null (reading statusCode)' and request_batch_api_success mismatches — triage these under the same envelope work (likely the paginated POST /messages response shape via the raw request() API).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Link headers use the ./resource?params form and ably-js paginates rest/history correctly
- [ ] #2 /time (and other REST endpoints) honour the msgpack Accept header
- [ ] #3 rest/history paginated tests and rest/request request_time pass
<!-- AC:END -->
