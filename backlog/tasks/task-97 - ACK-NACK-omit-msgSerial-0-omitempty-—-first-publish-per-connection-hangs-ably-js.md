---
id: TASK-97
title: >-
  ACK/NACK omit msgSerial 0 (omitempty) — first publish per connection hangs
  ably-js
status: To Do
assignee: []
created_date: '2026-07-12 13:57'
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
- [ ] #1 ACK and NACK frames always carry msgSerial, including msgSerial=0, in both JSON and msgpack
- [ ] #2 ably-js realtime/presence enter tests and realtime/updates-deletes serial tests pass against the provisioner harness
- [ ] #3 Empty-string message data round-trips as '' not undefined (publishVariations)
<!-- AC:END -->
