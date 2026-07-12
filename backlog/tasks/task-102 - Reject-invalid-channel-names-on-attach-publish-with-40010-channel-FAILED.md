---
id: TASK-102
title: 'Reject invalid channel names on attach/publish with 40010, channel FAILED'
status: To Do
assignee: []
created_date: '2026-07-12 13:58'
labels:
  - compat
  - ably-js
dependencies: []
priority: medium
ordinal: 102000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Ably rejects channel names with a leading colon (':hell', '::') with error 40010; the channel goes FAILED while the connection stays CONNECTED. The server accepts any name, so ~13 ably-js tests fail: realtime/channel channelattachinvalid (x4 variants), channelattach_publish_invalid (x4), channelattach_invalid_twice (x4) — attach succeeds where a 40010 error is expected — and realtime/failure failed_channel (attach to '::' should fail; also expects presence enter/get on the failed channel to return the channel-failed code). Fix: validate channel names at ATTACH and at realtime/REST publish (invalid leading ':' at minimum; check Ably's exact rules), reply with ERROR/attach-fail carrying code 40010 statusCode 400, and keep the connection open.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Attach to ':hell' fails with 40010, channel state is failed, connection remains connected
- [ ] #2 channelattachinvalid, channelattach_publish_invalid, channelattach_invalid_twice, and failed_channel pass
<!-- AC:END -->
