---
id: TASK-16
title: 'Always expose a channelSerial for a stream, even when the channel is empty'
status: To Do
assignee: []
created_date: '2026-05-31 16:11'
updated_date: '2026-05-31 16:11'
labels: []
dependencies:
  - TASK-13
ordinal: 16000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A stream/attachment should always have a channelSerial to return in ATTACHED — even for an empty channel — so the client always gets a resumable attach point. Today core.Stream.ChannelSerial() returns "" when parked at the sentinel (no ChannelMessage delivered yet). Since serial minting is being consolidated into storage, the empty-channel attach point should come from storage (the channel's current head/cursor serial), so a fresh attach to an empty channel still yields a non-empty channelSerial the client can later resume from. Depends on serial generation living in storage.
<!-- SECTION:DESCRIPTION:END -->
