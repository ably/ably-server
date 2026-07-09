---
id: TASK-5
title: Support multiple API keys (at least one required)
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 16:05'
updated_date: '2026-07-09 13:04'
labels:
  - auth
dependencies: []
ordinal: 5000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Today a single API key is configured via --api-key / ABLY_SERVER_API_KEY and internal/auth compares against exactly one key. Support configuring multiple keys (repeatable --api-key flag and/or comma-separated env), requiring at least one (startup fails with none). The Authenticator should accept a request matching ANY configured key (basic auth / ?key=). This is foundational for JWT auth, which selects the signing key by the JWT kid header.

All configured keys must belong to the same app — i.e. share the same appId (the `appId` component of the `appId.keyId:keySecret` format, before the `.`). The server exposes a single channel namespace, mirroring how one app on the Ably cloud platform owns one namespace of channels; keys spanning multiple appIds are a misconfiguration and startup must fail. Update DESIGN.md §3 (currently "A single API key").
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 At least one API key is required; startup fails if none is provided
- [x] #2 A request matching any configured key authenticates successfully
- [x] #3 All configured keys must share the same appId; startup fails if keys span multiple appIds
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. internal/config: add APIKeys []string (toml api-keys); keep singular APIKey. Helper to merge singular+plural.
2. cmd/ably-server/main.go: custom repeatable --api-key flag.Value; comma-split ABLY_SERVER_API_KEY env; file api-keys[]+api-key. Precedence flag>env>file as whole sets. Require >=1; parse each; all must share appId else startup fails.
3. internal/auth: Authenticator holds []APIKey + byName map. NewAuthenticator variadic. Authenticate matches ANY key (Basic/?key=) constant-time. verifyToken selects signing key by JWT kid header via VerificationKeySet (or fallback try-all). ValidateTokenRequest/MintToken select by tr.KeyName.
4. realtime/rest NewServer take []auth.APIKey; update call sites (incl tests).
5. DESIGN.md §3 and §9 updated. Tests: multi-key auth, kid selection, appId-mismatch startup failure.
<!-- SECTION:PLAN:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Support multiple API keys (TASK-5). config.File gains an api-keys array alongside the singular api-key; cmd/ably-server accepts a repeatable --api-key flag and a comma-separated ABLY_SERVER_API_KEY, resolving key sets with flag>env>file precedence. At least one key is required (startup fails with none) and all keys must share one appId (startup fails otherwise). auth.Authenticator now holds multiple keys: Basic/?key= matches any configured key in constant time; JWT verification selects the signing key by the kid header with a try-all fallback; ValidateTokenRequest/MintToken select the key by keyName. realtime/rest NewServer take []auth.APIKey. DESIGN §3 and §9 updated. Tests cover multi-key Basic auth, kid selection/mismatch/no-kid fallback, comma-separated env, and appId-mismatch startup failure.
<!-- SECTION:FINAL_SUMMARY:END -->
