---
id: TASK-104
title: Transcode binary payloads between msgpack and JSON representations
status: To Do
assignee: []
created_date: '2026-07-12 13:59'
updated_date: '2026-07-12 14:58'
labels:
  - compat
  - ably-js
dependencies: []
priority: high
ordinal: 104000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Message.Data is 'any' (internal/protocol/message.go); a binary payload published by a msgpack client decodes to []byte, and when fanned out to a JSON-format connection Go's encoding/json base64-encodes it WITHOUT appending the 'base64' step to Message.Encoding — the JSON subscriber decodes garbage. The same normalisation is missing on REST/history reads of binary messages in JSON format. Ably's rule: a binary payload delivered over JSON carries data as base64 with 'base64' appended to encoding; over msgpack it stays raw bytes (and an incoming JSON 'base64' payload delivered to a msgpack subscriber may be decoded to raw bytes with the suffix stripped). Failing tests (2026-07-12 run): realtime/crypto single_send_binary_text (msgpack publisher, JSON subscriber, encrypted payload mismatch), realtime/encoding message_encoding ('Check encodings match' — REST history read of fixture payloads published from both formats; also asserts stored encoding matches the canonical spec). Fix in the delivery/read path: normalise []byte data per outbound format for realtime fan-out, REST reads, and history.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A binary message published via msgpack is received intact by a JSON-format realtime subscriber with encoding ending in base64
- [ ] #2 crypto single_send_binary_text and encoding message_encoding pass
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Guidance from Lewis (2026-07-12): mirror the reference implementation. ../realtime/go/realtime/lib/ablyrpc/msgpack.go is a self-contained streaming MsgpackToJSON transcoder (631 lines, pooled buffers) — copy it verbatim (package rename; drop unused imports). The normalisation RULES live in ../realtime/go/realtime/lib/ablyrpc/data.go: DataFromExternal (ingress: outer-base64 payloads decoded to bin with the suffix stripped; msgpack maps/arrays canonicalised to JSON text via MsgpackToJSON), getOuterEncoding, and the egress split (AsStr base64-encodes bin for text transports; the base64 encoding suffix appended at JSON emission). Adapt data.go's rules to our Message.Data any representation rather than importing the protobuf Data union — but keep the semantics identical, including the empty-slice-not-nil quirk noted in AsAny (ably-js decode fails on msgpack nil data).
<!-- SECTION:NOTES:END -->
