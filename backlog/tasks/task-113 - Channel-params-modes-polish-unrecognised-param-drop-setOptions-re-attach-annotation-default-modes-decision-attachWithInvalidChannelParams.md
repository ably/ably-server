---
id: TASK-113
title: >-
  Channel params/modes polish: unrecognised-param drop, setOptions re-attach,
  annotation default modes decision (attachWithInvalidChannelParams)
status: To Do
assignee: []
created_date: '2026-07-12 16:54'
labels:
  - realtime
  - compat
  - ably-js
dependencies: []
priority: low
ordinal: 113000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Residual from TASK-103 (ably-js channel.test.js): attachWithInvalidChannelParams needs (a) unrecognised channel params dropped from the ATTACHED echo rather than echoed back, (b) setOptions-triggered re-attach to renegotiate params/modes, and (c) a decision on default modes: the reference's MODE_DEFAULT includes annotation_publish, ours deliberately excludes annotation modes from the no-flags default (DESIGN §4.2, agreed during the annotations design). Resolve (c) explicitly — either adopt the reference default or document the divergence and its test impact — then implement (a)/(b). Verify: npx mocha test/realtime/channel.test.js --grep attachWithInvalidChannelParams.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Unrecognised params are not echoed on ATTACHED
- [ ] #2 setOptions re-attach renegotiates params/modes
- [ ] #3 Default-modes divergence resolved and documented in DESIGN §4.2
<!-- AC:END -->
