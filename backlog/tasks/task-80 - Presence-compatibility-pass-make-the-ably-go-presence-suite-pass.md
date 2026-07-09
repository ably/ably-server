---
id: TASK-80
title: 'Presence compatibility pass: make the ably-go presence suite pass'
status: In Progress
assignee:
  - '@claude'
created_date: '2026-07-09 19:22'
updated_date: '2026-07-09 21:41'
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
- [ ] #1 All six listed presence tests pass against a local server
- [ ] #2 Any wire-shape divergences found are captured as regression tests in this repo
<!-- AC:END -->
