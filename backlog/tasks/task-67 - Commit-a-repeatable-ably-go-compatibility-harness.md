---
id: TASK-67
title: Commit a repeatable ably-go compatibility harness
status: To Do
assignee: []
created_date: '2026-07-09 11:06'
labels: []
dependencies: []
priority: high
ordinal: 67000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PDR-090's experimental release requires client-SDK test coverage via ably-go and/or ably-js, and the RFC frames the release as gated by conformance checks. The COMPAT_REPORT.md run was produced with throwaway, uncommitted hacks to ably-go's ablytest sandbox. Make that run repeatable: a committed harness (script or Go tooling in this repo, plus any upstreamable ably-go changes) that boots ably-server, runs the ably-go integration suite against it, and reports pass/fail per test with a known-failure allowlist so regressions are distinguishable from known gaps.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A single command boots the server and runs the ably-go integration suite against it locally
- [ ] #2 Known failures are recorded in an allowlist; the run exits nonzero on new failures
- [ ] #3 Documented so anyone can reproduce COMPAT_REPORT.md-style results
<!-- AC:END -->
