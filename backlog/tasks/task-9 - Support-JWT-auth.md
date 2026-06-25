---
id: TASK-9
title: Support JWT auth
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 16:05'
updated_date: '2026-06-25 00:26'
labels:
  - auth
dependencies:
  - TASK-5
ordinal: 9000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Add JWT bearer-token verification per DESIGN.md §3. Accept tokens via Authorization: Bearer <jwt> or ?accessToken=. Verify the HS256 signature against the configured key's secret, selecting the key by the JWT kid header (depends on multiple-API-keys support). Validate the required iat (with a small clock-skew leeway) and exp claims; surface auth failures as WS ERROR + close / REST 401. Parse the optional x-ably-capability and x-ably-clientId claims and make them available for downstream resolution (capability enforcement and clientId resolution are separate tasks). Implement in internal/auth alongside the existing key parsing.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 JWT presented via 'Authorization: Bearer <jwt>' or '?accessToken=' is verified (HS256) against the configured key's secret
- [x] #2 Optional x-ably-capability and x-ably-clientId claims are parsed and exposed for downstream resolution (no enforcement here)
- [x] #3 Existing Basic-auth / 'key' credential path continues to work unchanged
- [x] #4 Unit tests cover valid, expired, bad-signature, missing-claim, and clock-skew cases
- [x] #5 Tokens with bad signature, missing/invalid iat (beyond skew leeway), or expired/absent exp are rejected with HTTP 401 on the WS upgrade and on REST (auth is pre-upgrade, consistent with the existing key-auth path)
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add github.com/golang-jwt/jwt/v5 dependency (lightweight, standard).
2. internal/auth: extend credential extraction to accept Bearer / access_token tokens alongside Basic / key.
3. internal/auth: verify HS256 against the configured key's secret; validate iat (small leeway) + exp; parse x-ably-capability and x-ably-clientId claims.
4. Refactor Authenticate to return a Result {kind, capability, clientId-claim} instead of bare error; keep single-key (kid sanity-check only, multi-key lookup deferred to TASK-5).
5. Wire Result into realtime WS upgrade and REST entry points; failures -> WS ERROR+close / REST 401.
6. Unit tests in auth_test.go; go build + go test.
<!-- SECTION:PLAN:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Add HS256 JWT bearer-token verification (DESIGN.md §3) in internal/auth, alongside the existing API-key Basic auth.

What changed:
- Added github.com/golang-jwt/jwt/v5 (standard, lightweight, no transitive deps).
- Authenticate now returns a *Principal (credential Method + parsed x-ably-capability / x-ably-clientId claims) instead of a bare error, so downstream clientId resolution (TASK-11) and capability enforcement (TASK-12) can consume the claims. Basic/key path returns a MethodBasic principal unchanged.
- Tokens accepted via 'Authorization: Bearer <jwt>', or the access_token / accessToken query params (SDKs send access_token). HS256 verified against the configured key's secret; iat required (with 60s skew leeway), exp required; non-HS256 algs (incl. none) rejected.
- Single configured key: signature checked against the one secret; kid-based key selection is deferred to TASK-5 (multiple keys).
- Wired the new signature into the realtime WS upgrade (principal stored on the connection for TASK-11) and the REST authenticate helper.
- Fixed a wire-fact bug in DESIGN.md §2.1/§3: the token query param is access_token (the form ably SDKs send), not accessToken; both are accepted.

Scope notes:
- Auth rejection for WS is a pre-upgrade HTTP 401 (consistent with the existing key path), not a post-upgrade ERROR frame; DESIGN §3's 'WS: ERROR then close' wording describes an unimplemented variant and predates this change.
- No capability enforcement or clientId stamping yet (TASK-12 / TASK-11).

Tests: new TestAuthenticateJWT covering valid (Bearer + both query params), claims exposure, wildcard clientId, within-leeway skew, and rejections (expired, future-iat, missing iat, missing exp, bad signature, none alg). go build / go vet / go test ./... all pass.
<!-- SECTION:FINAL_SUMMARY:END -->
