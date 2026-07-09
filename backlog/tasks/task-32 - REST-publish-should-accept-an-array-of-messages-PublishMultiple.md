---
id: TASK-32
title: REST publish should accept an array of messages (PublishMultiple)
status: Done
assignee:
  - '@claude'
created_date: '2026-06-01 10:02'
updated_date: '2026-07-09 11:58'
labels:
  - protocol
dependencies: []
ordinal: 32000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
ably-go integration tests TestRESTChannel/PublishMultiple, TestRSL1f1 and TestRSL1g fail: POST /channels/{name}/messages with a JSON array body is rejected with 40000 Bad Request. Single-message publish works. Per Ably RSL1 the publish handler must accept either a single Message object or an array of Messages in one request, persisting/fanning out all of them. Implement in internal/rest.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 POST /channels/{name}/messages accepts a JSON array of messages and publishes all of them
- [x] #2 Single-message (object) publish continues to work
- [x] #3 Works for both application/json and application/x-msgpack bodies
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Array publish is already implemented in internal/rest: parseMessages + looksLikeArray decode either a single Message object or an array, for both application/json and application/x-msgpack, and all messages route through one ch.Publish => one Store => one atomic ChannelMessage. Existing tests cover JSON single, JSON array (atomicity), and msgpack single. Gap: no msgpack ARRAY test. Add TestPublishMsgpackArrayBody asserting a msgpack array body lands as one ChannelMessage carrying all messages (matching a WS MESSAGE frame's messages[]). Verify all ACs and mark done.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Array publish was already implemented (parseMessages/looksLikeArray in internal/rest/server.go handle a single object or an array for both application/json and application/x-msgpack; all messages route through one ch.Publish => one ChannelMessage). Added TestPublishMsgpackArrayBody to close the missing msgpack-array coverage; JSON single/array and msgpack single were already tested. No production code change needed.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Confirm and test that POST /channels/{name}/messages accepts either a single Message object or an array of Messages (RSL1) for both JSON and msgpack, landing all messages as one atomic ChannelMessage. The decode path (parseMessages + looksLikeArray) already supported this for both content types and routed every message through a single ch.Publish; the gap was test coverage for a msgpack array body, now added (TestPublishMsgpackArrayBody) alongside the existing JSON-array and single-message tests. No production changes were required.
<!-- SECTION:FINAL_SUMMARY:END -->
