---
id: TASK-87
title: Add an authenticated /stats stub returning an empty array
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 19:22'
updated_date: '2026-07-09 19:28'
labels:
  - rest
  - compat
dependencies: []
priority: high
ordinal: 87000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Stats collection is a DESIGN.md §1 non-goal, but the missing endpoint fails otherwise-healthy ably-go auth tests (TestAuth_BasicAuth, TokenAuth, TokenAuth_Renew, RequestToken and TestRestClient all trip on their Stats() call, per COMPAT_REPORT_2026-07-09.md) and returning 404 may be implicated in the TestRestClient hang. Add GET /stats as a compatibility stub: authenticate the request like any other REST endpoint, enforce the stats capability op, and return an empty paginated array (JSON/msgpack per Accept, standard Link convention with no next). Update DESIGN.md: §2.2 REST table row, §3.1 op table row, and the §1 non-goal wording to note the endpoint exists as an auth-checked empty stub — the server collects no statistics.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 GET /stats requires authentication and the stats capability op, returning 401/40160 respectively when unmet
- [x] #2 An authorised request returns 200 with an empty array, honouring Accept (JSON and msgpack)
- [ ] #3 The stats-tripped auth tests (TestAuth_BasicAuth token/basic variants, TokenAuth, RequestToken) get past their Stats() calls
- [x] #4 DESIGN.md §1, §2.2 and §3.1 updated
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Inspect internal/rest routing/auth helpers and the capability op set. 2. Add stats op to the §3.1 mapping. 3. Add GET /stats handler: auth + capability check, empty array response via the existing content negotiation. 4. Unit tests: 401 unauthenticated, 40160 without stats op, 200 empty array in JSON and msgpack. 5. Update DESIGN.md §1/§2.2/§3.1. 6. Full build/vet/test, commit.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implemented HandleStats in internal/rest: authenticate, then require the app-wide stats op via Capabilities().Permits("*", OpStats) — a channel-scoped stats grant (e.g. {"news:*":["stats"]}) is deliberately insufficient, matching stats being app-level. Empty array marshalled per Accept (json/msgpack). Added OpStats to internal/auth. Routed in newMux (instrumented like other REST routes). Updated the mux routes test, which had used /stats as its example unknown path.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Added GET /stats as an authenticated compatibility stub. The handler authenticates like every REST endpoint, requires the new app-wide stats capability op (granted on the * resource; 40160 error body otherwise, 401 unauthenticated), and returns an empty array honouring Accept (JSON/msgpack). Added auth.OpStats, routed the endpoint with HTTP metrics instrumentation, updated DESIGN.md (§1 non-goal wording now describes the stub, §2.2 table row, §3.1 op list + op table), and added TestStatsStub covering 401/40160/JSON/msgpack paths. AC#3 (ably-go auth tests get past Stats()) is left unchecked pending a suite re-run — expected to pass since the SDK's Stats() calls now receive 200 with an empty page.
<!-- SECTION:FINAL_SUMMARY:END -->
