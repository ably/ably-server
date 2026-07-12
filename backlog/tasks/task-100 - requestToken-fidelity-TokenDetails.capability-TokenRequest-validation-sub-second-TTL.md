---
id: TASK-100
title: >-
  requestToken fidelity: TokenDetails.capability, TokenRequest validation,
  sub-second TTL
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
ordinal: 100000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The requestToken path diverges from Ably in ways that fail ~25 ably-js auth/capability tests (rest/auth 16, rest/capability 7, realtime/auth authbase0, auth_jwt_with_subscribe_only_capability, auth_useAuthUrl_mixed_authParams_qsParams). Gaps, all verified against internal/rest/server.go HandleRequestToken and internal/auth/auth.go: (1) TokenDetails.capability is only echoed when the REQUEST carried one — Ably always returns the granted capability (the key's when unspecified), so tokenDetails.capability is undefined and JSON.parse(undefined) throws ('SyntaxError: undefined is not valid JSON' in ~10 tests); the capability returned should be the same narrowed value stamped into the JWT. (2) ValidateTokenRequest never validates timestamp (staleness), nonce (duplicate), or ttl (negative/excessive/invalid all accepted) — tests expecting 40xxx rejections get tokens. (3) MintToken writes exp at whole-second granularity, so ttl:1ms yields a token valid up to 1s — auth_expired_token_string then CONNECTS instead of failing (pair with TASK-98 for the failed-state mapping). (4) Invalid capability JSON is accepted (rest/capability 'Invalid capabilities 1-3'). (5) Capability intersection responses mismatch the expected narrowed JSON (ops/paths intersection tests). Also triage the 60s-timeout exchange flow auth_useAuthUrl_mixed_authParams_qsParams (signed TokenRequest reassembled from authParams/qsParams exchanged at /keys/{keyName}/requestToken) once validation lands.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 TokenDetails.capability always present and equal to the granted (narrowed) capability as a JSON string
- [x] #2 TokenRequest validation rejects bad timestamp, duplicate nonce, and negative/excessive/invalid ttl with Ably-shaped errors
- [x] #3 Sub-second ttl yields a token that is actually expired when presented after its expiry
- [ ] #4 rest/auth and rest/capability suites pass against the provisioner harness
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. TokenDetails.capability: MintToken returns the granted capability string; response always includes it — narrowed.String() when a capability was requested, else the key's original capability string verbatim (matches provisioner echo, avoids c14n array-order mismatch).
2. TokenRequest validation: capability structure/op-name (400), ttl negative/excessive(>24h)/non-numeric (400), timestamp staleness ±2min (401 40104), nonce replay within window (401 40105).
3. Sub-second exp: mint exp as fractional seconds and set jwt.TimePrecision=ms so a sub-second ttl actually expires; drop exp clock-skew leeway so an expired token is rejected with no grace.
4. Empty capability intersection -> 401 40160 (was 500).
5. Accept ttl/timestamp as JSON string or number (authUrl qs_to_body exchange).
6. Map token-request errors to Ably codes via auth.AuthErrorInfo; update DESIGN.md §3.3/§3.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Root causes & fixes (all in internal/auth/auth.go, internal/auth/capability.go, internal/rest/server.go):
- (1) capability echo: HandleRequestToken echoed the *requested* capability (or nothing when none requested → JSON.parse(undefined) threw). Now MintToken returns the granted capability: narrowed intersection (canonical, sorted) when requested, else APIKey.CapabilityString() (the key's original JSON verbatim). ably-js c14n sorts requested-capability arrays in place, so requested-capability assertions expect the sorted canonical form (matches narrowed.String()); the blanket/no-capability assertion isn't c14n'd, so it expects the key's verbatim form (matches capRaw echoed by the provisioner).
- (2) validation added: ValidateCapability (bad op name / '*'+op / empty op list / bad JSON → 40000/400), ValidateTTL (negative or >24h or non-numeric → 400), timestamp staleness (±2min → 40104/401), nonce replay (in-memory seen-set with window eviction → 40105/401).
- (3) sub-second exp: MintToken writes exp as fractional seconds; init() sets jwt.TimePrecision=time.Millisecond (default 1s truncated sub-second exp back to whole seconds); removed the 60s exp leeway (kept forward-only iat skew check) so an expired token is rejected outright. TokenDetails issued/expires unchanged (ms).
- (4) empty intersection now ErrCapabilityDenied → 40160/401 (previously ErrInvalidToken → 500; ably-js RSA4e normalised the code-less 500 to 401, so the old tests passed by accident).
- (5) TokenRequest.UnmarshalJSON accepts ttl/timestamp as number or numeric string (echo qs_to_body sends strings) → fixed auth_useAuthUrl_mixed_authParams_qsParams (was a 60s hang from repeated 400s).

Before/after (provisioner harness, ABLY_ENDPOINT=localhost:9080, TLS off):
- rest/capability.test.js: 7 passing / 7 failing → 14 passing / 0 failing.
- rest/auth.test.js: 16 passing / 16 failing → 28 passing / 4 failing. The 4 residual are echo.ably.io/embedded-JWT harness artifacts (COMPAT report §b), not requestToken fidelity: 'Rest embedded JWT' (+encryption) ECONNREFUSED to the remote echo service dialing :443; 'JWT request with invalid key' and 'Rest JWT with authCallback and invalid keys' expect 40400/404 multi-tenant app-id validation (non-goal, single-app server) and depend on echo.
- realtime/auth.test.js (--grep comet --invert): 66 passing / 0 failing / 4 pending. authbase0, auth_jwt_with_subscribe_only_capability, auth_useAuthUrl_mixed_authParams_qsParams, and auth_expired_token_string (web_socket binary+text) all pass. auth_expired_token_string comet variants remain red → TASK-99 (comet unsupported).

AC#4 left unchecked: rest/capability passes fully (14/0); rest/auth's 12 requestToken-fidelity failures are all fixed, but 4 residual remain that are NOT server requestToken gaps — 'Rest embedded JWT' (+encryption) and the two invalid-key JWT tests depend on the remote echo.ably.io service (ECONNREFUSED to :443 by construction) and multi-tenant app-id 40400 validation (a documented non-goal), classified as harness artifacts in COMPAT report §(b).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
requestToken fidelity: TokenDetails always returns the granted capability (narrowed canonical when requested, key's verbatim capability otherwise), so ably-js can always JSON.parse it. Added TokenRequest validation — capability op-name/structure and ttl range as 400s, stale timestamp (40104) and replayed nonce (40105) as 401s — and mapped empty capability intersection to 40160/401. Token exp is now sub-second (jwt.TimePrecision=ms, no exp grace) so short-ttl tokens expire when they should. ttl/timestamp accept string or number (authUrl exchange). DESIGN.md §3.3/§3 updated. rest/capability 7→0 failing, rest/auth 16→4 failing (4 residual are echo/multi-tenant harness artifacts), realtime/auth green (comet excluded).
<!-- SECTION:FINAL_SUMMARY:END -->
