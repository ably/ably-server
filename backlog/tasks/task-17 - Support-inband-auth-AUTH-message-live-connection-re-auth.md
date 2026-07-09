---
id: TASK-17
title: Support inband auth (AUTH message / live-connection re-auth)
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 16:11'
updated_date: '2026-07-09 13:27'
labels:
  - auth
dependencies:
  - TASK-9
ordinal: 17000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Implement inband re-authentication on an established WebSocket per Ably's AUTH flow. As a JWT token nears expiry, the server sends an AUTH message prompting the client to supply a fresh token. Handle the incoming AUTH message by: verifying the new token, checking the new credentials are compatible with the existing connection (same key / clientId constraints, capability), resetting the expiry timer to the new exp, and continuing the connection without disconnecting. On incompatible or invalid credentials, fail per protocol (ERROR / DISCONNECTED). Depends on JWT auth.
<!-- SECTION:DESCRIPTION:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. protocol: add ProtocolMessage.Auth *AuthDetails and AuthDetails{AccessToken} (wire {"action":17,"auth":{"accessToken":...}}).
2. auth: Principal.ExpiresAt from exp claim; exported VerifyToken(tokenString) wrapping verifyToken.
3. realtime connection: carry authn; guarded capability+tokenExpiry (authMu); capability() getter replaces principal.Capabilities() at all enforcement points. authLoop goroutine: for a token, at exp-preExpiryWindow send AUTH prompt; at exp with no valid re-auth send DISCONNECTED 40142/401 (writeLoop closes). Basic/zero-expiry: idle until a reauth signal.
4. dispatch ActionAuth -> handleAuth: verify inbound token, resolve clientId (no param) and require compatibility (== connection clientId), update capability+expiry, signal authLoop, reply CONNECTED w/ updated ConnectionDetails. Invalid token -> DISCONNECTED 40140/401; incompatible identity -> DISCONNECTED 40102/401.
5. DESIGN §2.1 AUTH row + §3 re-auth paragraph. Tests (run under -race): prompt-before-expiry, expiry disconnect 40142, successful inband re-auth -> CONNECTED + widened/narrowed cap, incompatible clientId rejected, invalid token rejected.
<!-- SECTION:PLAN:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Inband AUTH / live re-auth (TASK-17). ProtocolMessage gains an Auth *AuthDetails field ({"action":17,"auth":{"accessToken":...}}); Principal exposes the token ExpiresAt and the Authenticator a VerifyToken method. Each connection runs an authLoop: for a token it sends an AUTH prompt preExpiryWindow (30s) before exp and, absent a valid re-auth by exp, disconnects with DISCONNECTED code 40142 / status 401 (which ably-go treats as renewable). An inbound AUTH frame is verified, its resolved clientId required to match the connection's (§3.2 compatibility), and on success the connection's capability set and expiry are swapped in (guarded by a mutex, since the capability is also read on the publish worker) and a CONNECTED frame with updated ConnectionDetails is returned without dropping the connection. Invalid token -> DISCONNECTED 40140/401; incompatible clientId -> 40102/401. DESIGN §2.1 gains the AUTH row and §3 the re-auth paragraph. Tests (green under -race) cover the pre-expiry prompt, the token-expired disconnect, a successful re-auth that widens capability and outlives the original expiry, and the incompatible-clientId and invalid-token rejections.
<!-- SECTION:FINAL_SUMMARY:END -->
