---
id: TASK-89
title: Add a --fixtures flag to pre-seed channels from an Ably test-app-setup spec
status: To Do
assignee: []
created_date: '2026-07-09 21:51'
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
- [ ] #1 --fixtures loads a test-app-setup-shaped JSON and seeds each channel's presence members via StorePresence at startup
- [ ] #2 Seeded members appear in GET .../presence and presence history with clientId/data/encoding preserved verbatim
- [ ] #3 Seeded members survive indefinitely: no connection teardown, no cluster lease reaping
- [ ] #4 Invalid path or malformed spec is a startup error
- [ ] #5 DESIGN.md §9 and §12 updated
<!-- AC:END -->
