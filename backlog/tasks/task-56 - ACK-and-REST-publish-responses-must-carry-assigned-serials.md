---
id: TASK-56
title: ACK and REST publish responses must carry assigned serials
status: Done
assignee: []
created_date: '2026-06-14 21:40'
updated_date: '2026-06-14 21:46'
labels:
  - protocol
  - rest
  - realtime
dependencies: []
ordinal: 56000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The ably-go SDK reads an assigned-serials array from publish/mutation acknowledgements, but ably-server never sends one, so the SDK's serial-returning APIs come back empty.

Discovered while writing the mutable-messages SDK integration tests (TASK-55):
- ably-go reads serials from the WS ACK ProtocolMessage (proto field 'serials': []string, proto_protocol_message.go) and from the REST publish response ('serials' array, rest_channel.go ~L214). state.go onAckWithSerials populates the callback only when result.Serials is non-empty.
- Our protocol.ProtocolMessage has NO Serials field; the realtime publish ACK ({Action:Ack, MsgSerial, Count}) and the mutation ACK omit it, and the REST POST /messages response is an empty 201 with no body.

Effect: ably-go PublishWithResult().Serial is nil, RealtimeChannel.UpdateMessage/DeleteMessage's UpdateDeleteResult.VersionSerial is nil, and REST publish gives the caller no serial. Mutable-messages clients can't learn a message's serial from the publish result; they must read it from a subscription instead (the current TASK-55 integration test does this as a stopgap).

Scope:
- Add Serials []string (json+msgpack 'serials') to protocol.ProtocolMessage.
- Populate it on the realtime publish ACK with each persisted Message.Serial (in idx order), and on the mutation ACK with the new version serial.
- Return serials from REST POST /messages (response body) and from PATCH /messages/{serial} (already returns the version; align shape).
- Update the TASK-55 integration test to use PublishWithResult().Serial / UpdateDeleteResult.VersionSerial once available.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 protocol.ProtocolMessage has a Serials []string field (json+msgpack 'serials')
- [x] #2 Realtime publish ACK carries the persisted Message serials in idx order; ably-go PublishWithResult().Serial is populated
- [x] #3 Realtime update/delete/append ACK carries the new version serial; ably-go UpdateDeleteResult.VersionSerial is populated
- [x] #4 REST POST /messages returns the assigned serials; PATCH returns the version serial
- [ ] #5 TASK-55 integration test uses the SDK-returned serial instead of reading it from a subscription
<!-- AC:END -->
