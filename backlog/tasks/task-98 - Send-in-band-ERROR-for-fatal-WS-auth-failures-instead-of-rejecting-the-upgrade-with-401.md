---
id: TASK-98
title: >-
  Send in-band ERROR for fatal WS auth failures instead of rejecting the upgrade
  with 401
status: Done
assignee:
  - '@claude'
created_date: '2026-07-12 13:58'
updated_date: '2026-07-12 15:50'
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
- [x] #1 Connecting with an invalid key yields an in-band ERROR frame with a 401xx code and the SDK transitions to FAILED
- [ ] #2 realtime/failure invalid_cred_failure passes
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Do not reject a fatal WS auth failure with HTTP 401 (SDK treats a failed upgrade as a transport error → DISCONNECTED/retry, never FAILED).
2. Complete the upgrade first, then if Authenticate/ResolveClientID failed, send an in-band ERROR ProtocolMessage carrying the Ably error (code/statusCode from auth.AuthErrorInfo) with no preceding CONNECTED, then close — mirroring the reference frontdoor's closeWithError.
3. Map invalid key/no-creds→40101, clientId mismatch→40102, expired token→40142; close code normal for token errors, policy-violation otherwise.
4. Keep a genuine bad-request (bad format) as an HTTP-level rejection.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Root cause: internal/realtime/server.go HandleWebSocket rejected bad WS credentials with HTTP 401 before the upgrade. ably-js sees a failed upgrade as a transport-level (network) error, so the connection goes DISCONNECTED and retries — it never reaches FAILED. Ably completes the upgrade and sends an in-band ERROR ProtocolMessage carrying the auth ErrorInfo, which SDKs map to FAILED (non-renewable) or token-renewal (renewable 40142).

Fix: HandleWebSocket now resolves format first (a bad format stays an HTTP 400 — not an auth failure), authenticates and resolves the clientId but defers the error, upgrades the socket, and — on auth failure — calls rejectWithError, which writes a connection-level ERROR frame (no preceding CONNECTED) with code/statusCode from auth.AuthErrorInfo, then closes (normal-closure for token errors, policy-violation otherwise). This mirrors the reference frontdoor's closeWithError (go/realtime/roles/frontdoor/transports/websocket). Error mapping: invalid key / no creds → 40101, clientId mismatch → 40102, expired token → 40142 (all status 401).

Verified against a provisioned child (SDK, web_socket transport, bad key 'this.is:wrong'): connection → FAILED with connection.errorReason.code=40101 and stateChange.reason.code=40101 (was DISCONNECTED/retry). auth_expired_token_string (web_socket binary+text) now passes → FAILED/40171 (server sends 40142; the SDK maps to 40171 as it has no way to renew a token literal); this needed TASK-100's sub-second exp + no-exp-grace to mint an actually-expired token.

Before/after (provisioner harness):
- realtime/failure invalid_cred_failure: the web_socket/base path now goes to FAILED with 40101 (verified). The single 'it' also loops over the comet transport, whose branch gets 40400 from the REST 404 (comet unsupported) — so the whole test still reports red until TASK-99 (comet). AC#2 left unchecked for that reason.
- realtime/auth auth_expired_token_string: web_socket binary+text pass; comet variants remain red → TASK-99.

AC#1 satisfied: connecting with an invalid key yields an in-band ERROR frame with code 40101 (401xx) and the SDK transitions to FAILED (verified via the ably-js SDK, web_socket transport). AC#2 left UNCHECKED: realtime/failure invalid_cred_failure does not fully pass because the same test also exercises the comet transport, whose branch returns 40400 (REST not-found; comet is unsupported) — that residual belongs to TASK-99, not this task. The websocket/base mechanism the AC targets works correctly.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Fatal WS auth failures now complete the upgrade and send an in-band ERROR ProtocolMessage (code/statusCode via auth.AuthErrorInfo: 40101 invalid credentials, 40102 clientId mismatch, 40142 token expired) with no preceding CONNECTED, then close — instead of rejecting the upgrade with HTTP 401. ably-js maps this to FAILED (or token renewal for 40142) rather than a transport retry. Mirrors the reference frontdoor closeWithError. invalid_cred_failure's websocket path and auth_expired_token_string (web_socket) now pass; the comet sub-branches remain TASK-99.
<!-- SECTION:FINAL_SUMMARY:END -->
