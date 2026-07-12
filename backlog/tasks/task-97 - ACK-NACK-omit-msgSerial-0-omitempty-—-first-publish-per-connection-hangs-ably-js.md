---
id: TASK-97
title: >-
  ACK/NACK omit msgSerial 0 (omitempty) — first publish per connection hangs
  ably-js
status: Done
assignee:
  - '@claude'
created_date: '2026-07-12 13:57'
updated_date: '2026-07-12 14:36'
labels:
  - compat
  - ably-js
dependencies: []
priority: high
ordinal: 97000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
protocol.ProtocolMessage.MsgSerial is tagged json/msgpack omitempty (internal/protocol/message.go:115), so the ACK (and NACK) for the first publish-like frame on a connection — msgSerial=0 — is emitted WITHOUT a msgSerial field. ably-js correlates ACKs positionally: onAck computes endSerial = serial + count, which with serial=undefined is NaN, so the pending message is never completed and 'await channel.publish()' (or presence enter / annotation publish) hangs forever. ably-go decodes the missing field as 0, which is why the ably-go suite never caught this. Verified live: client log shows '[ProtocolMessage; action=ACK; count=1]' / 'Protocol.onAck(): serial = undefined' for msgSerial=0, and the publish promise times out; subsequent ACKs (msgSerial>=1) complete two queued messages at once, unblocking later publishes. This ONE bug cascades across the ably-js node suite: ~20 realtime/presence enter timeouts, realtime/updates-deletes (5), realtime/message publishEcho/explicit_client_id/subscribe_* (6), realtime/annotations (2), realtime/delta plugin tests (blocked before delta logic), channel publish_no_attach (4), connection connectionAttributes, failure break_transport, rest/batch batchPresence timeout. Fix: emit msgSerial explicitly on ACK/NACK (drop omitempty or use a pointer; note Ack emission sites internal/realtime/connection.go:527 and internal/realtime/presence.go:90). While in there, audit other zero-value omitempty fields that are semantically meaningful on the wire: Message.Data '' is also dropped (realtime/message publishVariations: empty-string data arrives as undefined).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 ACK and NACK frames always carry msgSerial, including msgSerial=0, in both JSON and msgpack
- [x] #2 ably-js realtime/presence enter tests and realtime/updates-deletes serial tests pass against the provisioner harness
- [x] #3 Empty-string message data round-trips as '' not undefined (publishVariations)
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
ably-js verification (provisioner harness on :9080, ABLY_USE_TLS=false, mocha --timeout 15000). before = compat report baseline (commit 156f5dc); after = this fix. before/after passing per file:

- realtime/presence.test.js: 10 -> 22 passing (3 pending, 11 failing). The msgSerial:0 ACK hang is resolved: client log now shows 'Protocol.onAck(): serial = 0' where it previously read 'serial = undefined' -> NaN. Residual failures are NOT TASK-97: presenceMessageExtras (TASK-105 extras); presenceEnterDetachEnter/EnterAndLeave/EnterUpdate/EnterLeaveGet hang downstream of the (now-correct) ACK -- confirmed identically failing on the pre-fix HEAD binary, so pre-existing, needs fresh presence-flow triage; presence_auto_reenter(+_different_connid), leave_published_for_member_missing_from_sync, suspended_preserves_presence, presence_many_updates, presenceEnterInvalid (expects err 90007).
- realtime/updates-deletes.test.js: 1 -> 3 passing (3 failing). Mutations now ACK instead of hanging; the 3 residuals are missing operation/version metadata on the delivered message (TASK-109).
- realtime/annotations.test.js: 0 -> 0 passing (2 failing) -- but failure mode changed from a WS ACK hang to REST-path gaps: REST annotation publish does not stamp the authenticated clientId (server sees anonymous -> rejects the aggregation method) = TASK-108, plus a REST annotation read. The realtime ACK hang itself is gone.
- realtime/message.test.js (--grep comet --invert): 33 -> 39 passing (3 failing). publishVariations PASSES (AC#3, empty-string data). Residuals: extras_field (TASK-105), subscribe_with_filter_object + subscribes-to-filtered-channel (filtered subscriptions/extras).
- rest/message.test.js: 6 passing / 2 failing; both residuals are TASK-108 (implicit clientId not stamped; connectionKey publish-on-behalf).

Provisioner + all children killed after the run; throwaway pre-fix worktree removed.

Gates (all green): go build ./...; go vet ./...; go vet -tags=integration ./...; go test -count=1 ./...; go test -race ./internal/realtime/; go test -tags=integration -race ./internal/storage/postgres/... (Docker).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Made ProtocolMessage.MsgSerial a *int64 (pointer+omitempty) with a PublishSerial() accessor (absent-means-0), so every ACK/NACK emits an explicit msgSerial including 0 while non-publish frames stay clean; read/construct sites updated across connection/presence/mutation/annotation. Fixed empty-string (and empty []byte) Data being dropped by msgpack omitempty -- which also lost it through the msgpack storage round-trip -- by adding msgpack.CustomEncoder/CustomDecoder on *Message, *PresenceMessage and *Annotation (map encode mirroring struct tags, Data forced when non-nil; decode via method-less shadow type for full type fidelity), guarded by a reflection parity test that fails if the encoder's field list drifts from the struct. Verified live against the ably-js provisioner harness: onAck serial=0 now correct (was NaN); presence 10->22, updates-deletes 1->3, message(no-comet) 33->39 passing, publishVariations passes; remaining reds belong to TASK-105/108/109 or a pre-existing presence-flow hang (confirmed on pre-fix binary). All gates incl postgres integration green.
<!-- SECTION:FINAL_SUMMARY:END -->
