---
id: TASK-90
title: >-
  Regression: TestRealtimeConn_ReconnectFromSuspendedState hangs (passed
  2026-07-09)
status: To Do
assignee: []
created_date: '2026-07-10 14:31'
labels:
  - realtime
  - compat
  - regression
dependencies: []
priority: high
ordinal: 90000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-10.md: this test passed on the 07-09 run and now hangs — confirmed on an isolated re-run (connection/channel never completes recovery; the 40s timeout converts it to a panic dump). A behaviour that worked yesterday broke, so this is fallout from one of the changes landed between the runs. Prime suspects, most-likely first: (1) TASK-86 f755772 — per-connection msgSerial monotonicity now DROPS non-monotonic MESSAGE/PRESENCE frames; a suspended-state reconnect re-sends queued publishes and this test may hit a sequence the tracker wrongly swallows (dropped frame = no ACK = SDK waits forever); (2) TASK-85 75cc73c — malformed resume/recover keys are now declined with error 80018; the suspended-state reconnect presents an old resume key and the SDK's handling of the new CONNECTED+error may loop; (3) TASK-81 77a1515 — repeat-ATTACH re-attach and ATTACH_RESUME handling. Bisecting the three against the test via scripts/ably-server-compat.sh -r '^TestRealtimeConn_ReconnectFromSuspendedState$' (ably-go checkout, read-only) should identify it quickly. Fix server-side; the test must return to passing without regressing RTL4/RTL6c2/RTN15c7, which those commits fixed.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Root cause identified and recorded (which commit/behaviour broke it and why)
- [ ] #2 TestRealtimeConn_ReconnectFromSuspendedState passes against a local server
- [ ] #3 RTL4_Attach, RTL6c2_PublishEnqueue, RTN15c7_attached and the RTN15a/b/d/e resume tests still pass (no re-regression)
<!-- AC:END -->
