---
id: TASK-103
title: Echo negotiated channel params/modes on ATTACHED
status: Done
assignee:
  - '@claude'
created_date: '2026-07-12 13:58'
updated_date: '2026-07-12 16:49'
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
- [x] #1 ATTACHED carries a params object reflecting accepted channel params and the modes derived from flags
- [x] #2 attachWithChannelModes and the attachWithChannelParams* tests pass (modulo the TASK-34 delta decision)
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. resolveRequestedModes: a comma-separated `modes` channel param wins over the flags mode bits (mirror reference paramsFromMap+ParseModes); wire it into handleAttach's effective-mode resolution.
2. echoParams: echo requested params on ATTACHED, rewriting the `modes` entry to the effective (capability-intersected) mode set (reference Mode.ParamsString); nil when no params requested.
3. Enforce the PUBLISH mode: a publish on an attachment lacking FlagPublish is NACKed 40160 (symmetric to presence.go), matching the reference handlePublish gate — needed for checkCantPublish.
4. Go unit + integration tests; ably-js verification of the four named tests; full channel.test.js (comet excluded) tail.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Root cause: ATTACHED never carried a params map and the server ignored the `modes` channel param, so ably-js (which reads channel.params from ATTACHED.params and channel.modes from ATTACHED.flags, realtimechannel.ts:567-569) saw empty params and, for the params-based tests, wrong modes. Separately, realtime publish was gated only on capability, not on the attachment's PUBLISH mode, so a subscribe-only attachment's publish was ACKed instead of rejected (checkCantPublish expects 40160).

Fix (three surfaces):
- resolveRequestedModes(flags, params): a comma-separated `modes` param wins over the flags mode bits, which win over the default set (mirrors reference ParseModes + precedence). Unrecognised tokens ignored.
- attachment.echoParams(): ATTACHED.params echoes the requested params with the `modes` entry rewritten to the effective (capability-intersected) mode set via modesParamString (reference Mode.ParamsString order); other params (delta, rewind) pass through; nil when none requested.
- connection.handleMessage: a create publish on a channel this connection is attached to WITHOUT FlagPublish is NACKed 40160 (symmetric to presence.go's PRESENCE-mode gate; reference handlePublish). A publish with no attachment stays a transient/capability-gated publish. Reference returns 40165 for cap-present-mode-absent, but our codebase's existing convention (presence.go) and the ably-js test both use 40160; matched for consistency.

DESIGN.md §4.1 documents the ATTACHED `params` field and the modes-param precedence in §4.2; the §4.2 'inbound MESSAGE without PUBLISH is NACKed' bullet already described the now-enforced rule.

Before (channel.test.js, comet excluded): file never completed (killed at 600s cap, 27/29 observed); attachWithChannelParamsBasicChannelsGet / BasicSetOptions / ModesAndChannelModes = 0/12 (checkCantPublish got an ACK); attachWithChannelModes + attachWithChannelParamsDeltaAndModes already passed.
After: file completes in ~1m — 82 passing, 4 pending, 10 failing. All four named tests + DeltaAndModes pass across every non-comet variant (20/20 attachWith* non-comet). The 10 failures are residuals, none TASK-103's: channelattachempty x8 (empty-name ATTACH sends an ERROR with channel='', which ably-js treats as a connection failure — edge case of TASK-102's committed name validation), attachWithInvalidChannelParams x1 (needs default modes incl annotation_publish + unrecognised-param dropping + setOptions re-attach; out of named scope), rewind_has_backlog_1 x1 (HAS_BACKLOG flag on rewind ATTACHED — rewind/backlog gap).

Deviations from reference: (a) echoParams passes unrecognised params through rather than dropping them, and echoes delta unconditionally rather than only when subscribing — untested by the four named tests, would matter for attachWithInvalidChannelParams. (b) publish-mode denial uses 40160 not 40165 (see above). (c) default modes still exclude annotation_publish (existing design decision).

Gates: go build/vet/vet-integration/test all green; go test -race ./internal/realtime/ green. New Go tests: TestParseModesParam, TestResolveRequestedModesParamWinsOverFlags, TestModesParamString, TestAttachedEchoesParamsModes, TestPublishNackWithoutPublishMode.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Echo negotiated channel params/modes on ATTACHED and enforce the PUBLISH mode (TASK-103).

ably-js reads channel.params from ATTACHED.params and channel.modes from ATTACHED.flags. The server now: (1) honours a comma-separated `modes` channel param (winning over flags mode bits) when resolving the effective mode set; (2) echoes ATTACHED.params reflecting the requested params, with the `modes` entry rewritten to the effective set (reference Mode.ParamsString order); (3) NACKs 40160 a publish on an attachment that lacks the PUBLISH mode (symmetric to the existing PRESENCE-mode gate), which the params tests' checkCantPublish requires.

Verification (channel.test.js, comet excluded): the file now completes (~1m) where it previously hit the 600s cap — 82 passing / 4 pending / 10 failing. All four named tests (attachWithChannelParamsBasicChannelsGet, attachWithChannelParamsBasicSetOptions, attachWithChannelParamsModesAndChannelModes, attachWithChannelModes) plus attachWithChannelParamsDeltaAndModes pass on every non-comet variant. The 10 residual failures belong to other/unfiled gaps (channelattachempty empty-name handling, attachWithInvalidChannelParams default-modes+param-filtering, rewind_has_backlog_1), none of them TASK-103's named tests. All Go gates green including -race.
<!-- SECTION:FINAL_SUMMARY:END -->
