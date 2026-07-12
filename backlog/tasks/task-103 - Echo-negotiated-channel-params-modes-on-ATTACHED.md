---
id: TASK-103
title: Echo negotiated channel params/modes on ATTACHED
status: In Progress
assignee:
  - '@claude'
created_date: '2026-07-12 13:58'
updated_date: '2026-07-12 16:06'
labels:
  - compat
  - ably-js
dependencies: []
priority: medium
ordinal: 103000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
ably-js attaches with ChannelOptions.params (e.g. {modes:'subscribe', delta:'vcdiff'}) and expects the ATTACHED frame to carry the negotiated params object and modes flags back (channel.params / channel.modes are populated from ATTACHED, RTL4k1/RTL4m). The server echoes mode FLAGS it accepts but never a params map, so realtime/channel attachWithChannelParamsBasicChannelsGet, attachWithChannelParamsBasicSetOptions, attachWithChannelParamsModesAndChannelModes, attachWithChannelModes (x4 variants each in the 2026-07-12 run) fail. Scope: echo the accepted params on ATTACHED (unsupported params should be omitted from the echo per spec); the delta:'vcdiff' VALUE itself rides TASK-34's decision — this task is only the params/modes echo mechanics.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 ATTACHED carries a params object reflecting accepted channel params and the modes derived from flags
- [ ] #2 attachWithChannelModes and the attachWithChannelParams* tests pass (modulo the TASK-34 delta decision)
<!-- AC:END -->
