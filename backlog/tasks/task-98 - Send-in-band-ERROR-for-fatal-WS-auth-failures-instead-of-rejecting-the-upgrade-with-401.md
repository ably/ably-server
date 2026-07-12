---
id: TASK-98
title: >-
  Send in-band ERROR for fatal WS auth failures instead of rejecting the upgrade
  with 401
status: To Do
assignee: []
created_date: '2026-07-12 13:58'
labels:
  - compat
  - ably-js
dependencies: []
priority: high
ordinal: 98000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
internal/realtime/server.go:69-74 rejects bad WebSocket credentials with HTTP 401 before the upgrade. ably-js treats a failed upgrade as a transport-level error → connection goes DISCONNECTED and retries, never FAILED. Ably's behaviour is to complete the upgrade and send an ERROR ProtocolMessage carrying the auth ErrorInfo (40101 invalid credentials, 40142 token expired, etc.), which SDKs map to the FAILED state (fatal, non-retryable). Failing ably-js tests: realtime/failure invalid_cred_failure ('connection state should be failed, not disconnected'), realtime/auth auth_expired_token_string (expects failed/40171 — also needs TASK-100's sub-second TTL fix to mint an actually-expired token). Fix: on Authenticate/ResolveClientID failure, upgrade the socket, emit ERROR with the right code/statusCode, then close.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Connecting with an invalid key yields an in-band ERROR frame with a 401xx code and the SDK transitions to FAILED
- [ ] #2 realtime/failure invalid_cred_failure passes
<!-- AC:END -->
