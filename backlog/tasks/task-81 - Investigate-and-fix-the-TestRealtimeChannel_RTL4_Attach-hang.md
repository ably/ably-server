---
id: TASK-81
title: Investigate and fix the TestRealtimeChannel_RTL4_Attach hang
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 19:22'
updated_date: '2026-07-09 21:10'
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
- [x] #1 The blocking attach scenario is identified and the server responds per protocol instead of hanging
- [x] #2 TestRealtimeChannel_RTL4_Attach passes (or its residual failure is a documented harness artifact, not a hang)
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Reproduce verbose; identify hanging RTL4 sub-behaviour.
2. Fix server so the scenario terminates per protocol (ATTACHED response, no hang).
3. Verify full test via harness; run go build/vet/test + race.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Root cause (RTL4g): handleAttach silently returned when an attachment already existed for the channel. RTL4g drives the channel to FAILED client-side (intercepting the server ATTACHED and rewriting it to an ERROR) while the server keeps the attachment, then re-Attaches. The SDK sends ATTACH and blocks for an ATTACHED; the server dropped the duplicate so the client hung in a reconnect loop until the per-test timeout. The reference (realtime connection.go attach) re-processes a duplicate ATTACH as an update-op that re-emits ATTACHED. Fix: treat a repeat ATTACH as a re-attach — stop the existing attachment and build a fresh one from the new ATTACH's modes/cursor/params, so the client always gets a new ATTACHED (+ any replay).

Second hang revealed after fixing g (RTL4j2, was masked because the per-test process died at g first): server ignored the ATTACH_RESUME flag (1<<5). channel1 attaches with rewind=1 AND SetAttachResume(true); it should receive 0 messages because a resume must not rewind, but the server applied rewind and delivered 1, so the test's chan1==0 waiter hung. Reference (channel/options.go NewOptions): isResume = channelSerial!="" || ATTACH_RESUME flag; rewind applied only when !isResume. Fix: added protocol.FlagAttachResume and suppress rewind in newAttachment when the flag is set (mirrors the existing channelSerial suppression).

Verification: full TestRealtimeChannel_RTL4_Attach PASSES via harness (all 21 subtests). go build/vet/test ./... green; go test -race ./internal/realtime ok. Updated DESIGN §4.1 (re-attach) and §4.3 (ATTACH_RESUME suppresses rewind).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Fixed the TestRealtimeChannel_RTL4_Attach hang (and a second hang it masked).

Root causes:
- RTL4g: handleAttach silently dropped a repeat ATTACH for an already-attached channel. The SDK re-sends ATTACH after moving the channel to FAILED locally and blocks for an ATTACHED, so it hung in a reconnect loop. Now a repeat ATTACH is a re-attach: the existing attachment is torn down and a fresh one built from the new ATTACH's modes/cursor/params, always yielding a new ATTACHED (matches the reference).
- RTL4j2: the server ignored the ATTACH_RESUME flag, so a rewind=1 + attach-resume attachment received a historic message it should not have, hanging the test. Added protocol.FlagAttachResume (1<<5) and suppress rewind on a resume attach (flag or channelSerial), mirroring the reference's isResume gate.

Changes: internal/realtime/connection.go (re-attach), internal/realtime/attachment.go + internal/protocol/message.go (ATTACH_RESUME/rewind), DESIGN.md §4.1 and §4.3.

Tests: full TestRealtimeChannel_RTL4_Attach passes via the ably-go compat harness (21/21 subtests); go build/vet/test ./... green; go test -race ./internal/realtime ok.

Risk/follow-up: none — behaviour now matches the reference. No new tasks needed.
<!-- SECTION:FINAL_SUMMARY:END -->
