---
id: TASK-99
title: 'Comet/HTTP transport: decide scope (ably-js node suite fans out over comet)'
status: To Do
assignee: []
created_date: '2026-07-12 13:58'
labels:
  - compat
  - ably-js
  - scoping
dependencies: []
ordinal: 99000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The ably-js node suite runs most realtime specs on both web_socket and comet (HTTP long-poll) transports via testOnAllTransportsAndProtocols. The server only implements WebSocket (DESIGN.md section 2.1), so every comet-pinned variant fails with 'requested resource not found' (the SDK POSTs to /comet-ish endpoints and gets our 404 ErrorInfo) or burns its full per-test timeout — 67+ failing variants counted across auth/channel/crypto/failure/message/reauth/resume full-run logs in the 2026-07-12 compat run, plus realtime/transports no_ws_connectivity and failure channel_backoff_comet which exist specifically to exercise comet fallback. Decision needed (mirrors TASK-34/TASK-35): implement a comet transport, or document WebSocket-only as an explicit non-goal in DESIGN.md section 1 and run SDK suites with comet variants excluded (--grep comet --invert). Note the harness impact: with comet included, channel.test.js and message.test.js exceed even a 600s per-file cap purely on comet timeouts.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Decision recorded: comet transport is in scope or an explicit documented non-goal
- [ ] #2 If non-goal: DESIGN.md section 1 documents it and the compat-run recipe excludes comet variants
<!-- AC:END -->
