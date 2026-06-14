---
id: TASK-58
title: NACK frames must carry Count for SDK ack-correlation
status: Done
assignee: []
created_date: '2026-06-14 22:09'
updated_date: '2026-06-14 22:10'
labels:
  - protocol
  - realtime
dependencies: []
ordinal: 58000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Realtime NACK frames omit Count, so the ably-go SDK cannot correlate them and the publishing call hangs until its context deadline instead of failing fast.

Discovered while writing the mutable-messages SDK integration tests (TASK-55): an UpdateMessage that the server NACKed caused the SDK call to block for minutes. ably-go's pending-message emitter computes 'count := msg.Count + serialShift' and acks 'queue[:count]' (state.go); our nack() helper and the inline publish NACKs set no Count, so Count=0 -> zero pending messages are resolved -> the operation's callback never fires.

Fix: every NACK (and ACK) must carry Count = the number of inbound frames it resolves (1 for a single publish/mutation/presence frame).

Scope:
- nack() helper sets Count: 1; inline publish NACKs in connection.go go through the helper.
- Note: the publish ACK currently sets Count = len(messages) (per-Message), but ably-go's pending queue is per-frame (per protocolMessage / msgSerial). Single-message publishes work, but batch publishes via the SDK would miscount. Reconcile ACK Count to per-frame semantics as a follow-up (kept out of this task to stay focused).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Every realtime NACK carries Count (1 for a single rejected frame)
- [x] #2 An SDK operation that is NACKed returns the error promptly instead of hanging to the context deadline
- [x] #3 Inline NACK constructions go through the shared nack() helper
<!-- AC:END -->
