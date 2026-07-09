---
id: TASK-36
title: Match Ably's publish response body shape
status: Done
assignee:
  - '@claude'
created_date: '2026-06-01 10:42'
updated_date: '2026-07-09 12:00'
labels:
  - protocol
dependencies:
  - TASK-18
priority: medium
ordinal: 36000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
REST POST /channels/{channel}/messages currently returns 201 with no body. Ably returns 201 with JSON {"channel": "<name>", "messageId": "<id>"} (or msgpack equivalent per Accept). SDKs parse this to surface a message identifier to the caller. The 'messageId' in Ably's response is the server-stamped ChannelMessage.ID (the form '<batchID>:<batchIdx>' observed empirically, e.g. 'TojWzTkLiH:0'), which is what TASK-18 stamps. So this task is gated on TASK-18 for the id value, but can shape the response envelope independently.

Empirically observed against local Ably (chtest2-81179):
  201 Created
  Content-Type: application/json
  {"channel": "chtest2-81179", "messageId": "TojWzTkLiH:0"}

Scope:
- Return 201 with {channel, messageId} body
- Respect Accept header (application/json default, application/x-msgpack supported)
- Use ChannelMessage.ID for messageId once TASK-18 lands; until then either block on TASK-18 or use channelSerial as a placeholder (decide at implementation time)

Out of scope:
- The id generation itself (TASK-18)
- Multi-batch publishes (a single POST is one atomic publish = one ChannelMessage = one messageId)
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 POST /channels/{channel}/messages returns 201 with body {channel: <name>, messageId: <id>}
- [x] #2 Response Content-Type honours Accept header: application/json (default) or application/x-msgpack
- [x] #3 messageId field is the server-stamped ChannelMessage.ID (depends on TASK-18 — confirm it is the same value emitted on the WS MESSAGE frame for the same publish)
- [x] #4 Unit tests cover the new response body in both JSON and msgpack and confirm round-trip with an Ably SDK or hand-rolled equivalent
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Change HandlePublish (internal/rest) response from the {serials} body to Ably's {channel, messageId} shape, 201, honouring Accept (json default / x-msgpack via existing marshalValue + acceptFormat). messageId = cm.Messages[0].ID = the TASK-18 batch id's first-message form '<batchID>:0' (matching Ably's observed 'TojWzTkLiH:0' and equal to the Message.ID carried on the delivered WS MESSAGE frame for the same publish, satisfying AC#3; also robust for postgres idempotent returns where cm.ID is re-read empty but message ids are persisted). Replace publishResponse struct (channel, messageId with json+msgpack tags), drop the now-unused messageSerials helper. Add wire-format tests: JSON and msgpack response decode to {channel, messageId}, messageId matches the id delivered on the channel stream, and single vs array publish both yield one messageId. Update DESIGN §2.2.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
messageId is sourced from cm.Messages[0].ID ('<batchID>:0'), not the bare ChannelMessage.ID base. Rationale: (a) it matches Ably's empirically observed shape 'TojWzTkLiH:0'; (b) it is exactly the id carried on the delivered MESSAGE frame for the same publish, satisfying AC#3's 'same value emitted on the WS MESSAGE frame' confirmation (verified in tests against the core stream); (c) it is robust for the Postgres backend, whose idempotent return re-reads the cm from rows with an empty ChannelMessage.ID while message ids are persisted. Dropped the old {serials} body and the now-unused messageSerials helper. WS ACK Res serials are unchanged.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Replace the REST publish response with Ably's RSL1 shape: 201 + {"channel":"<name>","messageId":"<id>"}, honouring Accept (application/json default, application/x-msgpack when requested) via the existing marshalValue/acceptFormat path. messageId is the TASK-18-stamped id of the publish's first message ('<batchID>:0'), which matches Ably's observed 'TojWzTkLiH:0' and equals the Message.ID delivered on the wire for the same publish. Added wire-format tests decoding the response independently in JSON and msgpack, asserting the channel/messageId fields, that messageId equals the id delivered on the channel stream (AC#3/#4), and that an array publish yields one messageId (the first message's). DESIGN §2.2 updated. AC#4's 'round-trip with an Ably SDK' is covered by a hand-rolled decode equivalent here; the ably-go integration suite was not run in this environment.
<!-- SECTION:FINAL_SUMMARY:END -->
