---
id: TASK-92
title: >-
  Fix presence history direction handling (TestPresenceHistory_Direction_RSP4b2
  panic)
status: To Do
assignee: []
created_date: '2026-07-10 14:31'
labels:
  - rest
  - presence
  - compat
dependencies: []
priority: medium
ordinal: 92000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-10.md gap 3: presence is otherwise green after the fixtures work, but TestPresenceHistory_Direction_RSP4b2 panics — newly visible now that --fixtures seeds members the test can page over (it did not fail in the TASK-80 verification runs on 07-09, and a bare direction=forwards query on an empty channel returns 200 at HEAD, so the failure needs seeded data and likely pagination to reproduce). Diagnose what the SDK receives for a direction-qualified presence-history read over the fixture data (wrong ordering, missing/odd Link header, or a response shape the SDK nil-derefs on) and fix HandlePresenceHistory to match message history's direction semantics. Reproduce/verify via scripts/ably-server-compat.sh -r '^TestPresenceHistory_Direction_RSP4b2$' (the script seeds fixtures by default; ably-go checkout is read-only).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Root cause of the panic recorded (what the SDK received)
- [ ] #2 TestPresenceHistory_Direction_RSP4b2 passes against a local server
- [ ] #3 Unit test in internal/rest pins direction-qualified presence-history ordering and pagination
<!-- AC:END -->
