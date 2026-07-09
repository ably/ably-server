---
id: TASK-83
title: Fix the JWT-as-token flow (TestAuth_JWT_Token_RSA8c hang)
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 19:22'
updated_date: '2026-07-09 20:57'
labels:
  - auth
  - compat
dependencies: []
priority: medium
ordinal: 83000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-09.md gap 5: the test mints an HS256 JWT signed with the key secret (via ably's echo server) and presents it as a token; the run hangs/panics. HS256 JWT verification is implemented (TASK-9), so diagnose the actual failure: whether the echo-server dependency makes this a harness artifact, whether the token transport (Authorization: Bearer base64(jwt) vs raw, ?access_token=) diverges, or whether verification rejects a claim shape the SDK produces. Fix server-side divergences; if the residual is purely the external echo-server dependency, document it as a harness artifact in the task.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 The failure mode is diagnosed and recorded
- [x] #2 TestAuth_JWT_Token_RSA8c passes, or its failure is documented as a harness artifact with the server side verified correct
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Reproduce TestAuth_JWT_Token_RSA8c against local server; determine if it reaches the server (echo.ably.io reachable, mints HS256 JWT signed with key secret, kid=appId.keyId, claims iat/exp only).
2. Check subtests: 'Get JWT from echo server' (pure echo), 'Should be able to use it as a token' (Bearer base64(jwt) -> /stats), authURL variants. Verify token transport + claim shapes our verifier accepts.
3. Fix real server-side gaps; if residual is external echo dep / harness plumbing (e.g. Stats() non-goal, MustSandbox), document as harness artifact with server side verified.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
DIAGNOSIS: TestAuth_JWT_Token_RSA8c is a HARNESS ARTIFACT — the server side is correct.

- echo.ably.io IS reachable from this environment. It mints an HS256 JWT signed with the key secret, header {alg:HS256, typ:JWT, kid:appId.keyId}, payload with ONLY iat/exp (no x-ably-capability/clientId). Our verifier accepts exactly this shape (kid selects the key; missing capability -> AllowAll; exp required and present).
- Why the test hangs/panics: every subtest builds its client with ably.NewREST(WithToken(jwt)/WithAuthURL(...), WithEndpoint(app.Endpoint)) WITHOUT the local-server overrides the harness only applies inside app.Options()/NewREST() (WithPort(9010)+WithTLS(false)+WithInsecureAllowBasicAuthWithoutTLS()). So the client uses TLS to a resolved Ably host and never reaches the plaintext local :9010 server. Subtest 'Should be able to use it as a token' blocks on a crypto/tls read; the per-test timeout is reported as a panic (the 'stall' the compat report noted). Subtest 'Get JWT from echo server' passes (pure echo, no server). This is the same class of artifact as RTN22/RTC8a4 (TASK-78).
- The test also assumes Stats() succeeds, which relies on the /stats stub (DESIGN §1 non-goal) returning 200 — it does (empty array).

SERVER SIDE VERIFIED CORRECT (direct curl vs the running working-tree server, using a live echo.ably.io-minted JWT):
  Authorization: Bearer base64(jwt)  (RSA3a)  -> HTTP 200
  Authorization: Bearer <raw jwt>              -> HTTP 200
  ?access_token=<jwt>                          -> HTTP 200
(The 400 seen on a first attempt was a macOS 'base64' 76-col line-wrap injecting newlines into my curl header, not the server.)

Pinned by a new unit test internal/auth/auth_test.go 'valid token via Authorization: Bearer base64 (RSA3a)', exercising the base64-Bearer transport with a minimal iat/exp token matching the echo-server shape.

Note (harness, read-only, not fixed here): ablytest CreateJwt has a latent nil-deref — on a failed request it calls res.Body.Close() with res==nil — so if echo.ably.io were unreachable this test would panic rather than fail cleanly. Not triggered here because the network is up.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Diagnosed TestAuth_JWT_Token_RSA8c as a harness artifact and verified the server-side JWT-as-token flow is correct.

Root cause: all of the test's subtests construct clients (ably.NewREST) without the local-server port/TLS overrides the harness applies only via app.Options()/NewREST(), so they talk TLS to a resolved Ably host and never reach the plaintext local :9010 server; subtest 2 blocks on a TLS read and the per-test timeout is reported as a panic. echo.ably.io is reachable and mints an HS256 JWT (kid=appId.keyId, minimal iat/exp claims) that our verifier accepts.

Server verified correct: a live echo-minted JWT is accepted on /stats via Bearer base64(jwt) (RSA3a), raw Bearer, and ?access_token= — all HTTP 200. Added unit test internal/auth/auth_test.go pinning the base64-Bearer transport with the minimal-claim echo shape.

No server code change was needed. AC#1 (failure mode diagnosed/recorded) and AC#2 (documented as harness artifact with server side verified correct) are both satisfied.
<!-- SECTION:FINAL_SUMMARY:END -->
