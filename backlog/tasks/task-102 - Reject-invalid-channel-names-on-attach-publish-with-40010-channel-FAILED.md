---
id: TASK-102
title: 'Reject invalid channel names on attach/publish with 40010, channel FAILED'
status: Done
assignee:
  - '@claude'
created_date: '2026-07-12 13:58'
updated_date: '2026-07-12 16:06'
labels:
  - compat
  - ably-js
dependencies: []
priority: medium
ordinal: 102000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Ably rejects channel names with a leading colon (':hell', '::') with error 40010; the channel goes FAILED while the connection stays CONNECTED. The server accepts any name, so ~13 ably-js tests fail: realtime/channel channelattachinvalid (x4 variants), channelattach_publish_invalid (x4), channelattach_invalid_twice (x4) — attach succeeds where a 40010 error is expected — and realtime/failure failed_channel (attach to '::' should fail; also expects presence enter/get on the failed channel to return the channel-failed code). Fix: validate channel names at ATTACH and at realtime/REST publish (invalid leading ':' at minimum; check Ably's exact rules), reply with ERROR/attach-fail carrying code 40010 statusCode 400, and keep the connection open.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Attach to ':hell' fails with 40010, channel state is failed, connection remains connected
- [x] #2 channelattachinvalid, channelattach_publish_invalid, channelattach_invalid_twice, and failed_channel pass
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add core.ValidChannelName mirroring the reference nameRegex (non-empty; first rune not ':' ',' whitespace or '['; no line-break runes).
2. Realtime ATTACH: reject invalid names with an ERROR frame (channel set, code 40010, status 400), no attachment, connection stays open.
3. Realtime MESSAGE publish: NACK invalid names with 40010/400.
4. REST publish: return Ably error body 40010/400.
5. Unit tests + ably-js verification (channelattachinvalid, channelattach_publish_invalid, channelattach_invalid_twice, failed_channel).
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Root cause: the server accepted any channel name at ATTACH and at publish, so invalid names (leading ':' etc.) were attached/published instead of rejected 40010.

Fix: added core.ValidChannelName mirroring the reference nameRegex (non-empty; first rune not ':' ',' ASCII-whitespace or '['; no line-break runes). Wired at three surfaces: realtime ATTACH -> ERROR 40010/400 (channel set, connection stays open, no attachment); realtime MESSAGE publish -> NACK 40010/400; REST publish -> Ably error body 40010/400. Leading '[' is rejected (qualified/[meta] channels are unsupported here) — noted as a deviation from the reference, which peels a bracket qualifier; no test exercises it.

Tests: core.TestValidChannelName; realtime TestAttachInvalidChannelNameErrors, TestPublishInvalidChannelNameNacks.

Before (comet excluded): channelattachinvalid/channelattach_publish_invalid/channelattach_invalid_twice = 0 passing / 12 failing; failed_channel = 0/1 failing. After: 12/12 passing; failed_channel 1/1 passing.

Gates: go build/vet/vet-integration/test all green; -race on internal/realtime green.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Reject invalid channel names with 40010 at attach and publish (TASK-102).

Root cause: the server accepted any channel name, so attaches/publishes to invalid names (leading ':' etc.) succeeded instead of failing with Ably 40010.

Change: added core.ValidChannelName, mirroring the reference channel-name predicate (non-empty; first rune not ':' ',' whitespace or '['; no line-break runes). Enforced at three surfaces:
- realtime ATTACH: ERROR frame with the channel and code 40010/400; no attachment; connection stays connected (SDK moves the channel to FAILED).
- realtime MESSAGE publish: NACK 40010/400.
- REST publish: Ably error envelope 40010/400.

A leading '[' is rejected because qualified/[meta] channels are not implemented here (a deviation from the reference, which parses a bracket qualifier; untested by the suite).

Verification (comet excluded): channelattachinvalid, channelattach_publish_invalid, channelattach_invalid_twice went 0/12 -> 12/12; failure/failed_channel 0/1 -> 1/1. Added Go unit + integration tests. All gates green including -race.
<!-- SECTION:FINAL_SUMMARY:END -->
