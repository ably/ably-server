---
id: TASK-112
title: >-
  Reject empty channel names without breaking the connection
  (channelattachempty)
status: To Do
assignee: []
created_date: '2026-07-12 16:54'
labels:
  - realtime
  - compat
  - ably-js
dependencies: []
priority: low
ordinal: 112000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Residual from the TASK-102/103 batch (ably-js channel.test.js, 8 variant failures): an ATTACH with an empty channel name gets an ERROR frame carrying channel='', which ably-js interprets as a connection-level error and fails the whole connection; the SDK expects a channel-scoped failure it can surface on the channel object. Check how the reference distinguishes channel-level vs connection-level ERROR for invalid/empty names (likely the error must still carry the channel field as sent, or a specific code) and mirror it. Verify via the worktree harness: npx mocha test/realtime/channel.test.js --grep channelattachempty.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 channelattachempty passes on all non-comet variants
- [ ] #2 An empty-name ATTACH does not terminate the connection
<!-- AC:END -->
