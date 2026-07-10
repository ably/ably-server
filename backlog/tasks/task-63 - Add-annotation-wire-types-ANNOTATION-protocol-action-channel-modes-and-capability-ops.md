---
id: TASK-63
title: >-
  Add annotation wire types, ANNOTATION protocol action, channel modes, and
  capability ops
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 11:05'
updated_date: '2026-07-10 10:24'
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
- [x] #1 Annotation type round-trips JSON and msgpack matching Ably's wire shape and enum values
- [x] #2 ProtocolMessage supports action ANNOTATION (21) carrying annotations[]
- [x] #3 ANNOTATION_PUBLISH and ANNOTATION_SUBSCRIBE mode flags participate in attach mode resolution alongside the existing four modes
- [x] #4 annotation-publish and annotation-subscribe are recognised capability ops
- [x] #5 Constants and wire shapes match DESIGN.md §14: annotation actions create=0/delete=1, ANNOTATION protocol action 21, mode bits ANNOTATION_PUBLISH 1<<21 and ANNOTATION_SUBSCRIBE 1<<22, type parsed as <name>:<aggregation> against the five v1 methods
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. protocol/action.go: add ActionAnnotation=21 (+name); add AnnotationAction type (create=0/delete=1) with names, no-omitempty. 2. New protocol/annotation.go: Annotation struct (id/serial/action/clientId/connectionId/type/name/messageSerial/count/data/encoding/timestamp) with json+msgpack tags pinned to Ably; the 5 v1 aggregation methods; anonymous-allowed set (multiple.v1,total.v1); ParseAnnotationType + Validate (messageSerial+type required, 40000 messages mirroring reference, anonymous-method allowance, count default 1 for multiple.v1). 3. protocol/message.go: add Annotations to ChannelMessage and ProtocolMessage; FlagAnnotationPublish=1<<21, FlagAnnotationSubscribe=1<<22. 4. auth/capability.go: OpAnnotationPublish/OpAnnotationSubscribe. 5. realtime attachment.go: split resolveModes into modeMask(6 bits) for masking vs defaultModes(4 bits, excludes annotation) for the no-bits default; connection.permittedModes adds annotation bits gated on the new caps. 6. Unit tests: annotation round-trip json+msgpack, action/enum constants, type validation cases, mode resolution incl. annotation opt-in + default-exclude.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Added protocol.Annotation (json+msgpack pinned to Ably field names; id/serial split per §8; action no-omitempty), ActionAnnotation=21, Annotations on ChannelMessage+ProtocolMessage, FlagAnnotationPublish=1<<21 / FlagAnnotationSubscribe=1<<22, OpAnnotationPublish/OpAnnotationSubscribe, and type validation (5 v1 methods, anonymous multiple.v1/total.v1 allowance, count-default-1 for multiple.v1, 40000 messages mirroring reference). resolveModes split into modeMask (6 bits, for masking requested) vs defaultModes (4 bits, the no-bits default EXCLUDING annotation modes per §4.2); permittedModes gates annotation bits on the new caps. Enforcement rides TASK-12 as before.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Annotation wire foundation: protocol.Annotation type + ANNOTATION action 21 + annotation mode flags (1<<21/1<<22, opt-in) + annotation-publish/annotation-subscribe capability ops + <name>:<aggregation> type validation. Unit tests cover json/msgpack round-trip, enum/flag constants, type parsing/validation, and mode resolution (default-excludes / opt-in). Matches DESIGN.md §14.
<!-- SECTION:FINAL_SUMMARY:END -->
