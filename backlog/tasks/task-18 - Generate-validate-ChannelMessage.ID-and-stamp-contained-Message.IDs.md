---
id: TASK-18
title: Generate/validate ChannelMessage.ID and stamp contained Message.IDs
status: To Do
assignee: []
created_date: '2026-05-31 16:11'
updated_date: '2026-05-31 16:11'
labels: []
dependencies:
  - TASK-13
ordinal: 18000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Add an ID field to protocol.ChannelMessage. On publish: if ChannelMessage.ID is unset, generate a random 8-character base64 string and set each contained Message.ID = "<id>:<idx>". If ChannelMessage.ID is set (client-supplied), validate that each contained Message.ID equals the expected "<id>:<idx>", rejecting/NACKing on mismatch. This batch id is the idempotency key indexed by storage (the bbolt `ids` sub-bucket / Postgres unique index on message id, DESIGN §6/§8). Implement in the publish path that is being consolidated under storage. Depends on the serial-generation consolidation task.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Unset ChannelMessage.ID yields an 8-char base64 id with each Message.ID set to "<id>:<idx>"
- [ ] #2 A client-supplied ChannelMessage.ID with mismatched contained Message.IDs is rejected
<!-- AC:END -->
