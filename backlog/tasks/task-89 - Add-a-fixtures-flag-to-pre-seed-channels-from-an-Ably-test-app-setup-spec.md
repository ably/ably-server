---
id: TASK-89
title: Add a --fixtures flag to pre-seed channels from an Ably test-app-setup spec
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 21:51'
updated_date: '2026-07-09 22:06'
labels:
  - config
  - presence
  - compat
dependencies: []
priority: high
ordinal: 89000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The ably-go presence tests (TestPresenceGet_*, TestPresenceHistory_*) expect the sandbox app's fixtures: common/test-resources/test-app-setup.json (the ably-common submodule) defines post_apps.channels — currently one channel, persisted:presence_fixtures, with 6 presence members of varying encodings (bool/int/string/json string data, one with encoding json, one cipher-encoded with encoding json/utf-8/cipher+aes-128-cbc/base64). The cloud sandbox seeds these at provisioning; the local server has no equivalent, so the tests read an empty presence set and panic. Add --fixtures <path> (env ABLY_SERVER_FIXTURES, plus the TOML config key) accepting a test-app-setup-shaped JSON file (the post_apps object or a bare {channels:[...]}): at startup, for each channel, enter its presence members through the normal StorePresence path (so they land in the membership set AND presence history) with server-synthesized connectionIds. Seeded members are static fixtures: exempt from connection-scoped teardown and, in cluster mode, from the liveness reaper (or given a non-expiring lease). Data/encoding must round-trip verbatim — the encoding field is opaque to the server. Document in DESIGN.md §9 (flag) and §12 (fixture members' liveness exemption), noting it exists for SDK test-suite compatibility.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 --fixtures loads a test-app-setup-shaped JSON and seeds each channel's presence members via StorePresence at startup
- [x] #2 Seeded members appear in GET .../presence and presence history with clientId/data/encoding preserved verbatim
- [x] #3 Seeded members survive indefinitely: no connection teardown, no cluster lease reaping
- [x] #4 Invalid path or malformed spec is a startup error
- [x] #5 DESIGN.md §9 and §12 updated
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. config.File: add Fixtures (toml "fixtures").
2. main.go: add ABLY_SERVER_FIXTURES env + --fixtures flag (config.Default precedence); after manager built, load+seed; any error => startup error (return 1).
3. New internal/fixtures pkg: Spec/Channel/Member types, Parse/Load (accept post_apps.channels or bare channels; verbatim data/encoding; validate name+clientId; malformed/empty => error), Seed(ctx, manager, spec, logger) entering each member via ch.PublishPresence(storage.WithStaticPresence(ctx), ...) with synthesized connectionIds.
4. storage: WithStaticPresence/IsStaticPresence context helpers.
5. postgres StorePresence: when static, upsert with node_id sentinel + expires_at 'infinity' so lease-bump/reaper never touch fixture rows.
6. DESIGN.md 9 (flag) + 12.5 (fixture liveness exemption).
7. Unit tests: fixtures.Parse (both shapes + malformed/empty), Seed into memory backend asserting Members + presence history + verbatim data/encoding, and startup-error path.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Added --fixtures/ABLY_SERVER_FIXTURES/TOML fixtures key wired through internal/config.

New internal/fixtures pkg: Parse (post_apps.channels or bare channels; unknown keys ignored; malformed/empty => error), Load, Seed (enters each member via ch.PublishPresence under storage.WithStaticPresence with synthesized connectionIds; data/encoding verbatim).

storage.WithStaticPresence/IsStaticPresence context marker; postgres StorePresence seeds fixture rows with sentinel node_id + 'infinity' lease so lease-bump/reaper never touch them.

Verified: booted memory server with --fixtures test-app-setup.json; TestRealtimePresence_Sync, TestRealtimePresence_EnsureChannelIsAttached, TestPresenceHistory_RSP4_RSP4b3 now pass (were FAIL/PANIC). Unit tests assert Members+history+encodings+startup error. Integration test TestStaticFixturePresenceSurvivesReaper passes (AC#3).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Add --fixtures flag to pre-seed presence members from an Ably test-app-setup spec (SDK test-suite compatibility).

What changed:
- internal/config + cmd/ably-server: --fixtures <path> (env ABLY_SERVER_FIXTURES, TOML key 'fixtures') resolved with the standard flag>env>file>default precedence.
- New internal/fixtures package: Parse/Load accept the full {post_apps:{channels:[...]}} shape or a bare {channels:[...]}, ignore unknown keys, and reject malformed/empty specs (no channels, unnamed channel, member with no clientId). Seed enters each channel's members through the normal ch.PublishPresence/StorePresence path so they land in both the membership set and presence history, with server-synthesized per-member connectionIds; clientId/data/encoding round-trip verbatim (encoding opaque, no cipher decode).
- storage.WithStaticPresence/IsStaticPresence context marker; postgres StorePresence stores fixture rows with a sentinel node_id and an 'infinity' lease so neither the lease-bump loop nor the reaper ever touches them. Memory/bbolt need no change (no reaper). Fixture members belong to no connection, so no teardown LEAVE.
- DESIGN.md §9 (flag) and §12.5 (static fixture liveness exemption) updated.

Tests:
- internal/fixtures unit tests: Parse both shapes + 5 malformed cases; Load bad-path error; Seed into a memory-backed manager asserting Members + presence history + verbatim data/encoding + distinct synthesized connectionIds.
- Integration: TestStaticFixturePresenceSurvivesReaper (postgres) — fixture member survives node crash + multiple reaper cycles.
- End-to-end: booted memory server with --fixtures test-app-setup.json; ably-go TestRealtimePresence_Sync, TestRealtimePresence_EnsureChannelIsAttached, and TestPresenceHistory_RSP4_RSP4b3 pass (previously FAIL/PANIC). The remaining PresenceGet_* failures are REST presence filtering/pagination, handled in TASK-80.
<!-- SECTION:FINAL_SUMMARY:END -->
