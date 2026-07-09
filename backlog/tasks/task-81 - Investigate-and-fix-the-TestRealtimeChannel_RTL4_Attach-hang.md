---
id: TASK-81
title: Investigate and fix the TestRealtimeChannel_RTL4_Attach hang
status: To Do
assignee: []
created_date: '2026-07-09 19:22'
labels:
  - realtime
  - compat
dependencies: []
priority: high
ordinal: 81000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-09.md gap 3: confirmed timeout, not a nil-deref — the channel Attach blocks and the connection sits in a reconnect loop until the 45s per-test timeout. A hang is worse than a clean error for SDK compatibility. Reproduce (the test exercises RTL4 attach behaviours, likely including attach-while-connecting or attach timeout paths), find the scenario the server never completes, and make it terminate with the protocol-correct response.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The blocking attach scenario is identified and the server responds per protocol instead of hanging
- [ ] #2 TestRealtimeChannel_RTL4_Attach passes (or its residual failure is a documented harness artifact, not a hang)
<!-- AC:END -->
