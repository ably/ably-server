---
id: TASK-63
title: >-
  Add annotation wire types, ANNOTATION protocol action, channel modes, and
  capability ops
status: To Do
assignee: []
created_date: '2026-07-09 11:05'
updated_date: '2026-07-10 10:13'
labels: []
dependencies: []
documentation:
  - 'https://ably.atlassian.net/wiki/spaces/product/pages/5171281935'
priority: high
ordinal: 63000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PDR-090 puts message annotations in the experimental-release functional scope ('mutable messages: edit/delete, annotations and summaries, appends'), but ably-server has none — internal/protocol/action.go deliberately omits the values. Add the wire foundation, pinned to realtime's constants: an Annotation type (id, action create(0)/delete(1), clientId, type, serial, messageSerial, name, count, data, encoding, timestamp, connectionId) carried as ChannelMessage.Annotations; ProtocolMessage action ANNOTATION (21); attachment mode flags ANNOTATION_PUBLISH / ANNOTATION_SUBSCRIBE; capability ops annotation-publish / annotation-subscribe added to the op set (enforcement itself rides TASK-12). Reference the realtime implementation: go/realtime/roles/frontdoor/protocol/protocol_message.go, lib/channel/params.go, lib/resource/operation.go, lib/ablyrpc/types.pb.go.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Annotation type round-trips JSON and msgpack matching Ably's wire shape and enum values
- [ ] #2 ProtocolMessage supports action ANNOTATION (21) carrying annotations[]
- [ ] #3 ANNOTATION_PUBLISH and ANNOTATION_SUBSCRIBE mode flags participate in attach mode resolution alongside the existing four modes
- [ ] #4 annotation-publish and annotation-subscribe are recognised capability ops
- [ ] #5 Constants and wire shapes match DESIGN.md §14: annotation actions create=0/delete=1, ANNOTATION protocol action 21, mode bits ANNOTATION_PUBLISH 1<<21 and ANNOTATION_SUBSCRIBE 1<<22, type parsed as <name>:<aggregation> against the five v1 methods
<!-- AC:END -->
