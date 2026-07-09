---
id: TASK-80
title: 'Presence compatibility pass: make the ably-go presence suite pass'
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 19:22'
updated_date: '2026-07-09 22:12'
labels:
  - presence
  - compat
dependencies: []
priority: high
ordinal: 80000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-09.md: presence is implemented and in scope, but the whole ably-go presence suite is red — FAIL: TestRealtimePresence_Sync, TestRealtimePresence_EnsureChannelIsAttached, TestPresenceGet_ConnectionID_RSP3a3; PANIC (nil-deref, suggesting an unparseable/missing response): TestPresenceGet_RSP3_RSP3a1, TestPresenceGet_ClientID_RSP3a2, TestPresenceHistory_RSP4_RSP4b3. Do a focused pass against the SDK's expectations: the SYNC wire shape and cursor protocol, REST GET .../presence and .../presence/history response envelopes and pagination, PresenceMessage field shapes, and connectionId/clientId stamping. Fix what diverges until the suite passes.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 All six listed presence tests pass against a local server
- [x] #2 Any wire-shape divergences found are captured as regression tests in this repo
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. TASK-89 fixtures un-PANIC PresenceGet/PresenceHistory and fix Sync/EnsureChannelIsAttached (fixture-dependent).
2. Remaining REST divergence: GET .../presence ignored limit/clientId/connectionId. Add parsePresenceQuery + paginateMembers (filter by clientId/connectionId, stable sort by Serial, limit + Link next). Reuse writeLinkHeaders.
3. Regression tests: presence Get pagination, clientId filter, connectionId filter.
4. Re-run all six target tests + TestRealtimePresence_* siblings against a --fixtures server; document any harness artifacts.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
TASK-89 seeding alone flipped TestRealtimePresence_Sync, TestRealtimePresence_EnsureChannelIsAttached and TestPresenceHistory_RSP4_RSP4b3 from FAIL/PANIC to PASS (verified booting a --fixtures server).

Root cause of remaining 3: GET .../presence returned the whole set, ignoring limit (RSP3a1 pagination), clientId (RSP3a2) and connectionId (RSP3a3) query params. SYNC path, presence-message wire shape and presence-history envelope were already correct.

Fix: HandlePresence now parses clientId/connectionId/limit/cursor (parsePresenceQuery) and paginates via paginateMembers (filter, stable sort by member Serial, limit + Link next reusing writeLinkHeaders). DESIGN §12.6 updated.

Verified against a locally-booted --fixtures server (memory mode, key abc123.xyz456:s3cr3t): all six target tests PASS — TestRealtimePresence_Sync, TestRealtimePresence_EnsureChannelIsAttached, TestPresenceGet_RSP3_RSP3a1, TestPresenceGet_ClientID_RSP3a2, TestPresenceGet_ConnectionID_RSP3a3, TestPresenceHistory_RSP4_RSP4b3.

Siblings re-run, no regressions: TestRealtimePresence_Presence_Enter_Update_Leave, TestRealtimePresence_ServerSynthesized_Leave, TestPresenceHistory_Direction_RSP4b2 all PASS.

Harness artifact: scripts/ably-server-compat.sh does not pass --fixtures, so the fixture-dependent tests (all but PresenceHistory_RSP4) only pass against a manually-booted --fixtures server, as the task anticipated. Regression tests added in internal/rest/presence_test.go (pagination + clientId/connectionId filters).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Make the ably-go presence suite pass.

Two root causes:
1. No fixture seeding — the PresenceGet/PresenceHistory/Sync/EnsureChannelIsAttached tests read the sandbox's persisted:presence_fixtures channel, which a local server had no way to populate; the reads returned empty and the SDK panicked. Resolved by TASK-89 (--fixtures).
2. GET .../presence ignored its query params — it returned the whole membership set regardless of limit (RSP3a1 pagination), clientId (RSP3a2) and connectionId (RSP3a3).

Changes (REST only; the SYNC protocol, presence-message wire shape and presence-history envelope were already correct):
- internal/rest/server.go: HandlePresence parses clientId/connectionId/limit/cursor (parsePresenceQuery) and applies paginateMembers — an exact-match clientId/connectionId filter, a stable sort by member serial, and a limit-bounded page whose trailing serial is the opaque next cursor, emitted via the shared writeLinkHeaders (Link rel=current/first/next).
- DESIGN.md §12.6 documents the presence Get filters and pagination.

Tests:
- internal/rest/presence_test.go: TestPresenceGetPaginatesByLimit, TestPresenceGetFiltersByClientID, TestPresenceGetFiltersByConnectionID.
- End-to-end against a --fixtures-booted local server: all six listed tests PASS (TestRealtimePresence_Sync, TestRealtimePresence_EnsureChannelIsAttached, TestPresenceGet_RSP3_RSP3a1, TestPresenceGet_ClientID_RSP3a2, TestPresenceGet_ConnectionID_RSP3a3, TestPresenceHistory_RSP4_RSP4b3), plus siblings (Presence_Enter_Update_Leave, ServerSynthesized_Leave, PresenceHistory_Direction_RSP4b2) with no regressions.

Note: the compat harness does not yet pass --fixtures, so the fixture-dependent tests must be verified against a manually-booted --fixtures server (as the task anticipated).
<!-- SECTION:FINAL_SUMMARY:END -->
