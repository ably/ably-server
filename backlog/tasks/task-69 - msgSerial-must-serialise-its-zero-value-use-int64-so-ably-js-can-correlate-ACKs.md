---
id: TASK-69
title: >-
  msgSerial must serialise its zero value (use *int64) so ably-js can correlate
  ACKs
status: To Do
assignee: []
created_date: '2026-06-30 18:58'
labels:
  - embedding-poc
  - sdk-compat
  - bug
dependencies: []
references:
  - EMBEDDING-POC.md
  - internal/protocol/message.go
ordinal: 69000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Surfaced by the embedding PoC (M2): ProtocolMessage.MsgSerial was int64 with json omitempty, which drops msgSerial=0 (the first publish on every connection) from the ACK. ably-js cannot correlate the ack to its pending publish and the publish promise hangs forever. Fix: make MsgSerial *int64 so a meaningful zero serialises (helpers Int64()/MsgSerialValue()). Working implementation on branch embed-poc-m2-node.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 an ACK/NACK for msgSerial=0 includes msgSerial on the wire
- [ ] #2 unmodified ably-js publish() resolves for the first publish on a connection
- [ ] #3 existing tests migrated to the pointer API with assertions unchanged; go test ./... green
<!-- AC:END -->
