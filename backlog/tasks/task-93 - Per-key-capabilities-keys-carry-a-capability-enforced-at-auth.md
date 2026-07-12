---
id: TASK-93
title: 'Per-key capabilities: keys carry a capability, enforced at auth'
status: Done
assignee:
  - '@claude'
created_date: '2026-07-12 10:39'
updated_date: '2026-07-12 10:50'
labels:
  - auth
  - config
dependencies: []
priority: high
ordinal: 93000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The provisioned test-app keys (ably-common test-app-setup.json) carry per-key capability JSON — subscribe-only, per-channel, wildcard-all — and ably-js's capability tests rely on restricted keys actually being restricted. Today every configured key is full-capability and narrowing happens only via JWT claims. Add: (1) a structured [[keys]] config-file section where each entry is the key string plus an optional capability (JSON object string, same format as x-ably-capability); --api-key / ABLY_SERVER_API_KEY keep working and default to full capability; (2) auth.APIKey carries the parsed Capability, and Basic auth (and ?key=) resolves the principal's capability to the KEY's capability instead of AllowAll; (3) JWT verification resolves the effective capability as claim ∩ signing key's capability (absent claim → the key's capability, per DESIGN §3's inheritance rule); requestToken minting already narrows (TASK-12) — verify it now narrows against the per-key capability. Update DESIGN §3/§9. This is prerequisite work for the sandbox provisioner (child servers boot with the six spec keys).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Config [[keys]] entries accept key + optional capability; flag/env single-key path unchanged and full-capability
- [x] #2 Basic auth with a restricted key is denied ops outside its capability (40160 semantics unchanged)
- [x] #3 A JWT signed by a restricted key resolves to claim ∩ key capability; absent claim inherits the key capability
- [x] #4 requestToken narrowing operates against the signing key's own capability
- [x] #5 DESIGN.md §3 and §9 updated
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. auth: APIKey gains an unexported cap field; Capability() returns it. ParseAPIKey defaults to AllowAll (flag/env path unchanged). Add ParseAPIKeyWithCapability(spec, capJSON) parsing the x-ably-capability JSON (empty => AllowAll).
2. auth: matchKey returns the matched APIKey; Basic/?key= principal resolves cap to the key's Capability() (not AllowAll).
3. auth: verifyToken captures the signing key (kid, else the key whose secret verifies the signature via token.Method.Verify) and sets principal cap = keyCap when claim absent, else claimCap.Intersect(keyCap). MintToken already narrows against key.Capability() (now per-key) => AC#4.
4. config: add [[keys]] section (KeyEntry{key, capability}); combine with api-key/api-keys in the file tier.
5. main: resolveAPIKeys returns key+capability specs; parse via ParseAPIKeyWithCapability.
6. DESIGN §3/§9 updates.
7. Tests: capability parse, restricted Basic denied end-to-end (REST 40160 + WS attach intersection), JWT claim ∩ key, requestToken narrowing against per-key cap, config [[keys]] parse.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Config schema: [[keys]] array of KeyEntry{key, capability} where capability is an x-ably-capability-format JSON string (empty => full capability). Combined with api-key/api-keys within the file tier; flag/env keys stay full-capability.

auth.APIKey now carries an unexported cap; ParseAPIKey defaults to AllowAll, ParseAPIKeyWithCapability parses the JSON. Basic/?key= resolves matchKey's matched key capability. verifyToken captures the signing key (kid, else the key whose secret verifies the sig) and sets principal cap = keyCap (absent claim) or claim.Intersect(keyCap). MintToken already narrowed against key.Capability(), now per-key.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Per-key capabilities enforced at auth (DESIGN §3/§9).

Changes:
- config: new structured [[keys]] TOML section (KeyEntry{key, optional capability as an x-ably-capability JSON string}); combined with api-key/api-keys within the file tier. Flag/env single-key path unchanged and full-capability.
- auth.APIKey carries a parsed Capability; ParseAPIKey defaults to full, ParseAPIKeyWithCapability parses the JSON (malformed => error).
- Basic auth (and ?key=) resolves the principal to the matched KEY's capability instead of AllowAll (matchKey now returns the matched key).
- JWT verification captures the signing key (via kid, else the key whose secret verifies the signature) and resolves the effective capability as claim ∩ key capability; an absent claim inherits the key capability.
- requestToken minting (MintToken) already narrowed against key.Capability(), which is now per-key; confirmed by test.
- main.go resolveAPIKeys returns key+capability specs parsed via ParseAPIKeyWithCapability.

Tests: capability parse; restricted Basic key denied end to end (REST 40160 + WS attach-mode intersection); JWT claim ∩ key and absent-claim inheritance; requestToken narrowing against a scoped key; config [[keys]] parse; malformed-capability startup error. go build/vet/vet -tags=integration/test all pass; go test -race ./internal/realtime passes.
<!-- SECTION:FINAL_SUMMARY:END -->
