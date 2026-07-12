---
id: TASK-100
title: >-
  requestToken fidelity: TokenDetails.capability, TokenRequest validation,
  sub-second TTL
status: To Do
assignee: []
created_date: '2026-07-12 13:58'
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
- [ ] #1 TokenDetails.capability always present and equal to the granted (narrowed) capability as a JSON string
- [ ] #2 TokenRequest validation rejects bad timestamp, duplicate nonce, and negative/excessive/invalid ttl with Ably-shaped errors
- [ ] #3 Sub-second ttl yields a token that is actually expired when presented after its expiry
- [ ] #4 rest/auth and rest/capability suites pass against the provisioner harness
<!-- AC:END -->
