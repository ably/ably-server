---
id: TASK-94
title: Config-native keys/namespaces/channel fixtures; retire --fixtures
status: Done
assignee:
  - '@claude'
created_date: '2026-07-12 10:39'
updated_date: '2026-07-12 10:57'
labels:
  - config
  - presence
dependencies:
  - TASK-93
priority: high
ordinal: 94000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Per Lewis (2026-07-12): everything the server starts with should be visible in one config file, structured similarly to the test-app-setup post_apps shape, with the provisioner writing config rather than the server parsing the fixtures JSON. Extend the TOML config with: [[namespaces]] (id plus feature flags persisted/mutableMessages/pushEnabled — accepted and recorded, behaviourally inert for now, documented as such); [[channels]] with nested presence member entries (clientId, data, encoding verbatim) seeded at startup through the existing fixtures machinery (static-member liveness exemptions unchanged, DESIGN §12.5). Remove the --fixtures flag and ABLY_SERVER_FIXTURES (landed 2026-07-10, only consumer is the ably-go compat script). Update the ably-go compat script (server-testing branch — cross-repo, Lewis-approved direction) to generate a tmp TOML from ably-common's test-app-setup.json instead of passing --fixtures. Update DESIGN §9 (config reference) and §12.5.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Channels + presence members declared in the TOML config are seeded at startup exactly as --fixtures did (data/encoding verbatim, static liveness)
- [x] #2 [[namespaces]] entries parse and are recorded; documented as inert
- [x] #3 --fixtures / ABLY_SERVER_FIXTURES removed; malformed config sections are startup errors
- [x] #4 ably-go compat script generates and passes a tmp config; fixture-dependent presence tests still pass through it
- [x] #5 DESIGN.md §9 and §12.5 updated
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. config: add [[namespaces]] (Namespace{id, persisted, mutableMessages, pushEnabled} — parsed/retained, inert) and [[channels]] (Channel{name, presence []PresenceMember{clientId, data, encoding}}). Remove Fixtures field.
2. fixtures: drop JSON Load/Parse (parser leaves the server); keep Spec/Channel/Member + Seed (static-member exemptions unchanged).
3. main: remove --fixtures flag, ABLY_SERVER_FIXTURES, fixturesEnv. Build a fixtures.Spec from config.Channels (validate: channel name + member clientId required; namespace id required) and seed it. Malformed sections => startup error.
4. DESIGN §9 (drop --fixtures; add [[namespaces]] documented inert + [[channels]]) and §12.5 (static members now config-seeded).
5. Tests: config parse (namespaces/channels), seed from config, startup errors; drop fixtures Parse/Load tests.
6. ably-go compat script (server-testing branch, that file only): python3 JSON->TOML for channels/presence, boot with --config; keep -f/--fixtures path semantics ('' disables). Verify TestPresenceGet_RSP3_RSP3a1 + TestPresenceHistory_RSP4_RSP4b3 PASS. Commit separately with Co-Authored-By.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Config schema: [[namespaces]] = Namespace{id, persisted, mutableMessages, pushEnabled} (parsed/retained, inert — no behaviour keys off the flags). [[channels]] = Channel{name, presence []PresenceMember{clientId, data, encoding}}; data typed as a TOML string (all test-app-setup presence data are JSON strings), opaque. Removed the Fixtures config field.

fixtures pkg: dropped JSON Load/Parse (parser leaves the server) and the testdata JSON; kept Spec/Channel/Member + Seed. main.fixtureSpec validates (namespace id, channel name, member clientId) and translates config.Channels -> fixtures.Spec; malformed sections => startup error. Removed --fixtures flag + ABLY_SERVER_FIXTURES.

ably-go server-testing branch: scripts/ably-server-compat.sh now emits a tmp TOML from test-app-setup.json via python3 and boots with --config (kept -f/--fixtures semantics). Committed separately (a460fb2), only that file; untracked docs/ left alone.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Config-native fixtures; retire --fixtures (DESIGN §9, §12.5).

Changes:
- config: new [[namespaces]] section (id + persisted/mutableMessages/pushEnabled) — parsed and retained but behaviourally inert, documented as such; and [[channels]] with nested [[channels.presence]] members (clientId/data/encoding, verbatim/opaque). Removed the Fixtures field.
- fixtures: dropped the JSON parsing path (Load/Parse) and its testdata; kept Spec/Channel/Member and the Seed machinery (static-member liveness exemptions unchanged).
- main: removed the --fixtures flag and ABLY_SERVER_FIXTURES; added fixtureSpec() which validates the new sections (namespace id, channel name, member clientId all required — malformed => startup error) and seeds from the config's channels through the existing fixtures.Seed.
- ably-go (server-testing branch, separate commit): scripts/ably-server-compat.sh translates test-app-setup.json into a temporary TOML config via python3 and boots the server with --config, keeping the -f/--fixtures option semantics.

Tests: config parse for namespaces/channels; seed-from-spec verbatim data/encoding; fixtureSpec validation + a malformed-channel startup-error run test. go build/vet/vet -tags=integration/test all pass. End-to-end smoke: booting with a [[channels]] config seeds presence (GET /presence returns members with data/encoding verbatim). Compat: scripts/ably-server-compat.sh -r '^TestPresenceGet_RSP3_RSP3a1$|^TestPresenceHistory_RSP4_RSP4b3$' => both PASS.
<!-- SECTION:FINAL_SUMMARY:END -->
