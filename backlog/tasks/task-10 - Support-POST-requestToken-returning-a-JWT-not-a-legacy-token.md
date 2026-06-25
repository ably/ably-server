---
id: TASK-10
title: Support POST /requestToken returning a JWT (not a legacy token)
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 16:05'
updated_date: '2026-06-25 01:32'
labels:
  - auth
dependencies:
  - TASK-9
ordinal: 10000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Implement a token-request endpoint that mints and returns a JWT (HS256, signed with the matching key's secret, kid header set) rather than an Ably legacy TokenRequest/TokenDetails. Honour the requested ttl, capability (-> x-ably-capability claim) and clientId (-> x-ably-clientId claim), constrained by the authenticating key's own capability. Returns the signed JWT for SDKs to use as accessToken. Depends on the JWT auth infrastructure. Note: confirm the exact path and request/response shape SDKs expect for a JWT-returning token endpoint before finalising.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 POST /keys/{keyName}/requestToken mints an HS256 JWT (kid=keyName, signed with the key secret) honouring requested ttl/capability/clientId
- [x] #2 A signed TokenRequest is authenticated by recomputing HMAC-SHA256 over keyName\nttl\ncapability\nclientId\ntimestamp\nnonce\n and constant-time comparing the mac; mismatch -> 401
- [x] #3 An unsigned request is accepted when authenticated by Basic auth with the same key; absent mac and no matching Basic -> 401
- [x] #4 keyName not matching the configured key -> 401
- [x] #5 DESIGN.md §3 updated: the server now issues tokens via requestToken (it previously stated verify-only)
- [x] #6 Unit tests cover mac-valid, mac-mismatch, basic-auth-unsigned, and wrong-key; ably-go TestAuth_RequestToken exercised against the server
- [x] #7 The JWT is returned inside a TokenDetails JSON body (token field; ably-go rejects application/jwt on this REST endpoint) and is usable as an access_token, including the SDK's base64-encoded 'Authorization: Bearer base64(jwt)' form (verified by the TASK-9 path)
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Implement POST /keys/{keyName}/requestToken (DESIGN.md §3.3): mint an Ably-compatible token for a key holder.

What changed:
- auth.TokenRequest + ValidateTokenRequest: a signed request is authenticated by recomputing base64(HMAC-SHA256(keyName\nttl\ncapability\nclientId\ntimestamp\nnonce\n, keySecret)) and constant-time comparing the mac; an unsigned request is accepted only with Basic auth for the same key; wrong key / bad mac / no-mac-no-basic -> 401.
- auth.MintToken: issues an HS256 JWT signed with the key secret (kid=keyName), iat/exp from ttl (default 60m), x-ably-capability / x-ably-clientId when supplied, and the request nonce as jti so distinct requests yield distinct tokens.
- rest.HandleRequestToken returns the JWT inside a TokenDetails JSON body (honouring Accept json/msgpack). Registered POST /keys/{keyName}/requestToken.

Drive-by fix surfaced by the SDK: ably-go presents REST tokens as 'Authorization: Bearer base64(jwt)' (RSA3a). TASK-9's extractToken now base64-decodes the Bearer value (falling back to raw), which was required for any REST token auth to work, not just requestToken.

Validation: validated against the real ably-go suite. TestAuth_RequestToken's mac check, token minting, token uniqueness, and token-auth round-trip all pass against a local server; the test's only remaining failures are its rest.Stats() calls (/stats is a documented non-goal). Added auth unit tests: ValidateTokenRequest (valid/tampered/wrong-key/unsigned-with-basic/unsigned-without-basic) and MintToken round-trip (verifies via base64 Bearer + nonce uniqueness).

Scope/notes: requested capability is recorded but not narrowed (enforcement is TASK-12); nonce replay tracking not implemented (mac guarantees integrity). DESIGN §2.2/§3 updated (endpoint added; 'does not issue tokens' corrected; Bearer base64 documented).
<!-- SECTION:FINAL_SUMMARY:END -->
