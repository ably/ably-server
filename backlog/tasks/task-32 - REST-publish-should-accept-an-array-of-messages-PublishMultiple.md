---
id: TASK-32
title: REST publish should accept an array of messages (PublishMultiple)
status: To Do
assignee: []
created_date: '2026-06-01 10:02'
updated_date: '2026-06-03 13:06'
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
- [ ] #1 POST /channels/{name}/messages accepts a JSON array of messages and publishes all of them
- [ ] #2 Single-message (object) publish continues to work
- [ ] #3 Works for both application/json and application/x-msgpack bodies
<!-- AC:END -->
