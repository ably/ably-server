---
id: TASK-36
title: Match Ably's publish response body shape
status: To Do
assignee: []
created_date: '2026-06-01 10:42'
labels: []
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
- [ ] #1 POST /channels/{channel}/messages returns 201 with body {channel: <name>, messageId: <id>}
- [ ] #2 Response Content-Type honours Accept header: application/json (default) or application/x-msgpack
- [ ] #3 messageId field is the server-stamped ChannelMessage.ID (depends on TASK-18 — confirm it is the same value emitted on the WS MESSAGE frame for the same publish)
- [ ] #4 Unit tests cover the new response body in both JSON and msgpack and confirm round-trip with an Ably SDK or hand-rolled equivalent
<!-- AC:END -->
