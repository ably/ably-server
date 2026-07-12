---
id: TASK-114
title: >-
  Capability matcher: support the [qualifier]resource wildcard (e.g. [*]*) used
  by the sandbox all-access key
status: To Do
assignee: []
created_date: '2026-07-12 17:36'
labels:
  - compat
  - ait
dependencies: []
priority: high
ordinal: 114000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
THE blocker for running the AIT SDK (and any standard Ably SDK sandbox suite) against ably-server via the provisioner (TASK-96).

## Symptom
The ably-common test-app-setup.json all-access key — keys[5], capability `{ "[*]*": ["*"] }` — is denied on EVERY channel. ably-ai-transport-js authenticates all clients with keys[5], so all 61 integration tests fail immediately at attach/publish with `insufficient capability` (40160). ably-js escaped this only because its harness uses keys[0] (undefined capability → the provisioner defaults it to `{ "*": ["*"] }`, the plain wildcard, which the server DOES match).

## Evidence (live, against a provisioned child)
- keys[0] `{"*":["*"]}`  → `POST /channels/foo/messages` and `/channels/mutable:foo/messages` both 201.
- keys[5] `{"[*]*":["*"]}` → same publishes 401: `insufficient capability: \"publish\" required for channel \"foo\"`.
- Switching the AIT harness to keys[0] flips the suite from 0/61 to 59/61 passing.

## Root cause
`internal/auth/capability.go:matchResource` (and `intersectPath`) split the resource pattern on `:` and only treat a bare `*` segment as a wildcard. The Ably resource-qualifier syntax `[qualifier]resource` is not parsed, so `[*]*` is treated as a single literal segment and matches no channel. DESIGN §3.1 explicitly scopes out `[queue]*`/`[meta]*` (no queues/metachannels here) but is silent on the `[*]` (any-qualifier) wildcard, which the standard sandbox all-access key relies on.

## Suggested direction (do not fix in TASK-96)
Recognise the `[qualifier]resource` prefix in the capability matcher; at minimum `[*]` (any qualifier) must match normal (unqualified) channels so `[*]*` grants everything, matching `{"*":["*"]}` for the in-scope channel surface. Update DESIGN §3.1 to state the `[*]` qualifier is honoured (and how `[queue]`/`[meta]` qualifiers are treated). Representative: ably-common keys[5]; every AIT integration spec.
<!-- SECTION:DESCRIPTION:END -->
