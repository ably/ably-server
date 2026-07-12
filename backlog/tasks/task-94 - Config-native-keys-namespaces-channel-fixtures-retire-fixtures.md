---
id: TASK-94
title: Config-native keys/namespaces/channel fixtures; retire --fixtures
status: To Do
assignee: []
created_date: '2026-07-12 10:39'
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
- [ ] #1 Channels + presence members declared in the TOML config are seeded at startup exactly as --fixtures did (data/encoding verbatim, static liveness)
- [ ] #2 [[namespaces]] entries parse and are recorded; documented as inert
- [ ] #3 --fixtures / ABLY_SERVER_FIXTURES removed; malformed config sections are startup errors
- [ ] #4 ably-go compat script generates and passes a tmp config; fixture-dependent presence tests still pass through it
- [ ] #5 DESIGN.md §9 and §12.5 updated
<!-- AC:END -->
