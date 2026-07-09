---
id: TASK-85
title: 'Investigate remaining resume sub-scenarios (RTN15c6, RTN15c7_attached, RTN16)'
status: To Do
assignee: []
created_date: '2026-07-09 19:22'
labels:
  - realtime
  - compat
dependencies: []
priority: medium
ordinal: 85000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-09.md: most resume/rewind tests now pass, but RTN15c6 fails and RTN15c7_attached / RTN16 remain red. RTN16 exercises connection recovery (recover=), which DESIGN.md §4.3 deliberately treats as a no-op — decide whether the SDK tolerates a designed-no-op recovery (if not, either implement the minimal recovery response the protocol requires or document the test as an accepted incompatibility). RTN15c6/c7 exercise resume-with-attached-channels responses (CONNECTED vs re-ATTACHED expectations) — diagnose against the SDK and fix the server's post-resume behaviour where it genuinely diverges.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Each of the three tests is diagnosed with a root cause recorded in the task
- [ ] #2 Genuine server divergences are fixed; any accepted incompatibility is documented in DESIGN.md §4.3
<!-- AC:END -->
