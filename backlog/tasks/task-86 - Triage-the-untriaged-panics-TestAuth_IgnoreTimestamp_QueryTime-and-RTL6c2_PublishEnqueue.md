---
id: TASK-86
title: >-
  Triage the untriaged panics: TestAuth_IgnoreTimestamp_QueryTime and
  RTL6c2_PublishEnqueue
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 19:22'
updated_date: '2026-07-09 21:39'
labels:
  - compat
dependencies: []
priority: low
ordinal: 86000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-09.md gap 8: both panic with the nil-deref-against-unimplemented pattern and were not individually triaged this run. TestAuth_IgnoreTimestamp_QueryTime exercises authWithQueryTime (token requests timestamped from GET /time — implemented, so the panic needs explaining); RTL6c2_PublishEnqueue exercises publish-while-connecting queueing (mostly SDK-side). Re-run each in verbose mode, identify the missing/misshapen server response, file or fix accordingly.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Both tests have a recorded root cause
- [x] #2 Server-side gaps are fixed or spun into their own tasks; harness artifacts documented
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Reproduce both tests verbosely against the local server.
2. For each: identify root cause (server gap vs harness artifact).
3. Fix small in-scope server gaps here; document harness artifacts with evidence.
4. Verify via harness + go build/vet/test + race.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
TestAuth_IgnoreTimestamp_QueryTime = HARNESS ARTIFACT (not a server gap). The test passes ably.WithTLS(true) in its options; ablytest.Sandbox.Options merges caller opts AFTER the harness's local-server overrides (MergeOptions(appOpts, opts)), so WithTLS(true) wins over the harness's WithTLS(false). The client therefore dials https://localhost:9010 — TLS against the plaintext local server — and the request never completes (50s timeout). Same class as the RSA1 subtest of TestAuth_BasicAuth already noted in the compat report. Server side is fine: verified GET /time and POST /keys/{keyName}/requestToken both return correct responses over plaintext (queryTime timestamp + a valid TokenDetails JWT). No server change; test-AC left unchecked as a documented harness artifact.

TestRealtimeChannel_RTL6c2_PublishEnqueue = GENUINE SERVER GAP, now fixed. It was flaky (~1 in 3-8 runs panicked 'Ack called but queue is empty'), which is why the single-run compat pass tagged it PANIC. Root cause via server debug logging: on a reconnect the SDK re-flushes a queued publish with the SAME msgSerial (0), sending the identical MESSAGE frame twice on the new connection; the server ACKed both, but the SDK tracks only one pending entry per msgSerial, so the second ACK hit an empty queue and panicked. The reference server's checkMsgSerial drops a non-monotonic msgSerial (default: drop, don't close). Fix: acceptMsgSerial tracks the connection's last publish msgSerial (init -1, shared by MESSAGE+PRESENCE like the SDK's single counter) and drops a repeated/backward frame on the read goroutine before publish/ACK; forward skips allowed.

Verification: RTL6c2 25/25 green (was flaky); harness sweep of RTL6*/RTN15*/RTN19*/message-updates/RTL4 all PASS except RTN15c6 (documented resume non-goal, TASK-85) and RTN22a ServerInitiatedAuth (pre-existing TASK-17 PANIC, out of scope). go build/vet/test ./... green; go test -race ./internal/realtime ok. Updated the server unit test TestWSMutationOwnership (its sendUpdate helper reused msgSerial=1 for alice's second publish — now parameterised to stay monotonic). Documented the drop in DESIGN §5.2. No new tasks needed.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Triaged both untriaged panics.

TestAuth_IgnoreTimestamp_QueryTime: harness artifact, not a server gap. The test forces ably.WithTLS(true), which the sandbox harness merges after its own WithTLS(false) override, so the client dials TLS against the plaintext local server and hangs to the timeout. The server side of the queryTime token flow is fully implemented — verified GET /time and POST requestToken return correct responses over plaintext. No server change; documented as a harness artifact (same class as TestAuth_BasicAuth/RSA1).

TestRealtimeChannel_RTL6c2_PublishEnqueue: genuine (flaky) server gap, fixed. On reconnect the SDK re-flushes a queued publish with the same msgSerial, sending the MESSAGE frame twice; the server ACKed both and the SDK's second ACK hit an empty pending queue and panicked. Added per-connection msgSerial monotonicity tracking (acceptMsgSerial): a repeated/backward publish/presence msgSerial is dropped on the read goroutine without re-publish or re-ACK (forward skips allowed), mirroring the reference server's checkMsgSerial. Confirmed 25/25 green.

Changes: internal/realtime/connection.go (acceptMsgSerial + dispatch gate), internal/realtime/server.go (lastMsgSerial init -1), internal/realtime/capability_test.go (sendUpdate msgSerial param so TestWSMutationOwnership stays monotonic), DESIGN §5.2.

Tests: harness sweep of publish/ACK/resume/message-update tests all pass (bar the documented RTN15c6 non-goal and the pre-existing RTN22a TASK-17 panic); go build/vet/test ./... green; go test -race ./internal/realtime ok.

Follow-ups: none required.
<!-- SECTION:FINAL_SUMMARY:END -->
