---
id: TASK-34
title: Decide scope and support for the delta=vcdiff channel param
status: To Do
assignee: []
created_date: '2026-06-01 10:04'
updated_date: '2026-06-03 13:06'
labels:
  - scoping
dependencies: []
ordinal: 34000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
ably-go TestRealtime_ChannelParams_DeltaSupport fails: the server does not honour the delta=vcdiff channel param at attach time (the delta plugin pass-through tests TestDelta_PluginBasicFunctionality and TestDeltaPluginRecovery do pass). Decision needed: is delta encoding in scope? If yes, negotiate the delta channel mode on ATTACH and emit delta-encoded MESSAGEs with correct vcdiff base/encoding. If no, document it as a non-goal in DESIGN.md and close.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Decision recorded: delta is in scope or an explicit non-goal
- [ ] #2 If in scope: delta=vcdiff is negotiated on ATTACH and TestRealtime_ChannelParams_DeltaSupport passes
<!-- AC:END -->
