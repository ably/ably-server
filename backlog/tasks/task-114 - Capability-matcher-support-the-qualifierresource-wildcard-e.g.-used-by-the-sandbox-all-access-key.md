---
id: TASK-114
title: >-
  Capability matcher: support the [qualifier]resource wildcard (e.g. [*]*) used
  by the sandbox all-access key
status: Done
assignee:
  - '@claude'
created_date: '2026-07-12 17:36'
updated_date: '2026-07-12 17:45'
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

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Parse Ably [qualifier]resource syntax in internal/auth/capability.go: split a leading [qualifier] prefix from the resource name; qualifier type token mirrors the reference (QualifierType.Matches: equal, or either is '*'). Requests are plain channel names (default qualifier '').
2. Rework matchResource to (a) parse the pattern's qualifier + path, (b) require the pattern qualifier to match the default ('') request qualifier, (c) run the existing segment pathsMatch. So [*]* matches any channel, [meta]* matches none, plain patterns unchanged.
3. Rework Intersect/intersectPath to carry qualifiers: intersect qualifier types per reference (wildcard on either side yields the other; differing concrete types -> no intersection), intersect paths, and re-encode the joined resource with a [qual] prefix when non-default.
4. Refine DESIGN.md §3.1: qualifiers are parsed and matched per Ably semantics; [*] matches any type (so [*]* grants everything a plain channel needs); [queue]/[meta] parse but match nothing since those resources don't exist.
5. Tests: extend matcher/intersect table tests (incl. exact keys[5] string) and the provisioner e2e test to exercise keys[5].
6. Gates + AIT suite run (keys[5]) 0/61 -> ~59/61; record numbers.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implemented in internal/auth/capability.go: parseResource splits a leading [qualifier] prefix (qualifier TYPE token) from the ':'-delimited name path, mirroring reference resource.ParseID/ParseQualifier. qualifierMatches mirrors QualifierType.Matches (equal, or either '*'). matchResource now requires the pattern qualifier to match the default '' request qualifier (plain channels) then runs pathsMatch on segments; so [*]* matches any channel, [*]foo matches foo, and [meta]*/[queue]* match no channel. Intersect carries qualifiers via intersectQualifier ([*] on either side yields the other; differing concrete types don't intersect) and re-encodes via encodeResource, so §3.3 token narrowing of keys[5] against a concrete request keeps working. Plain-pattern semantics unchanged.

DESIGN.md §3.1 refined: qualifiers are parsed/matched per Ably semantics; [*] matches any type so [*]* == plain all-access over the channel surface; [queue]/[meta] parse but match nothing (no such resources); intersect qualifier rule documented.

Tests: extended matcher table ([*]*, [*]foo, [*]chat:*, [meta]*/[queue]* negatives), added exact keys[5] {"[*]*":["*"]} Permits case (publish/subscribe/history/presence on arbitrary channels), Intersect narrowing cases ([*]* ∩ concrete, [*]* ∩ [*]*, [meta]* ∩ [queue]* empty), and extended cmd/ably-sandbox e2e TestProvisionAndSmoke to publish with keys[5] on an arbitrary channel (201).

Gates (all green): go build ./...; go vet ./...; go vet -tags=integration ./...; go test -count=1 ./...; go test -race ./internal/realtime.

AIT suite (worktree server-testing @44f04093, committed keys[5] harness), VITE_ABLY_PROVISION_URL=http://localhost:9080/apps pnpm test:integration, provisioner on :9080 with freshly built /tmp/ably-bin binaries: BEFORE 0/61, AFTER 60/61. The single residual is durable-cross-process 'supersedes an abandoned partial step from a fresh process' — a deterministic TASK-115 item (retry-supersede fidelity), not TASK-114. The previously-flaky agent-session suspend/resume passed this run (so 60/61, one better than the ~59/61 predicted). Per-file: agent-session 17/17, client-session 24/24, durable-cross-process 6/7, codec 11/11, error-propagation 2/2.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Capability matcher now parses and matches Ably's [qualifier]resource syntax, mirroring the reference resource matcher. The sandbox all-access key keys[5] {"[*]*":["*"]} — whose [*] wildcard qualifier matches any resource type — now grants every op on plain channels, unblocking the AIT SDK suite. [meta]/[queue] qualifiers parse but match no channel (no such resources). Intersect/token-narrowing (§3.3) carries qualifiers per the reference. AIT suite: 0/61 → 60/61 (sole residual is the deterministic TASK-115 supersede test). DESIGN §3.1 refined. All gates green.
<!-- SECTION:FINAL_SUMMARY:END -->
