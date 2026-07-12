---
id: TASK-93
title: 'Per-key capabilities: keys carry a capability, enforced at auth'
status: To Do
assignee: []
created_date: '2026-07-12 10:39'
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
- [ ] #1 Config [[keys]] entries accept key + optional capability; flag/env single-key path unchanged and full-capability
- [ ] #2 Basic auth with a restricted key is denied ops outside its capability (40160 semantics unchanged)
- [ ] #3 A JWT signed by a restricted key resolves to claim ∩ key capability; absent claim inherits the key capability
- [ ] #4 requestToken narrowing operates against the signing key's own capability
- [ ] #5 DESIGN.md §3 and §9 updated
<!-- AC:END -->
