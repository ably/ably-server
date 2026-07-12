---
id: TASK-111
title: >-
  Fix the presence enter/leave flow hang (presenceEnterAndLeave, EnterUpdate,
  EnterLeaveGet)
status: To Do
assignee: []
created_date: '2026-07-12 14:58'
labels:
  - presence
  - compat
  - ably-js
dependencies: []
priority: high
ordinal: 111000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Found during TASK-97 verification: ably-js realtime/presence.test.js tests presenceEnterAndLeave, presenceEnterUpdate and presenceEnterLeaveGet hang to the per-test timeout. Confirmed pre-existing (identical failure on the pre-TASK-97 HEAD binary) and NOT part of the msgSerial cascade — after TASK-97, presence went 10->22 passing and these are among the residuals. Reproduce against the provisioner harness (ably-js worktree at ably-js-server-testing, mocha --timeout 15000 test/realtime/presence.test.js), diagnose what the SDK is waiting for in the enter->leave flow (candidate areas: LEAVE ack/delivery ordering, presence action transitions, the leave-with-data path), and fix. The remaining presence residuals not covered here belong to their filed tasks — check the 2026-07-12 ably-js compat report classification first.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 presenceEnterAndLeave, presenceEnterUpdate and presenceEnterLeaveGet pass against the provisioner harness
- [ ] #2 Root cause recorded; regression-pinned by a unit test in internal/realtime
<!-- AC:END -->
