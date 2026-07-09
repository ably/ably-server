---
id: TASK-69
title: Run the protocol conformance suite against ably-server as a release gate
status: To Do
assignee: []
created_date: '2026-07-09 11:06'
labels: []
dependencies: []
references:
  - 'https://github.com/ably/realtime/issues/8524'
documentation:
  - >-
    https://ably.atlassian.net/wiki/spaces/~5ef9be47c796b50bb2e1ae5c/pages/5226397709
priority: high
ordinal: 69000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The shared-protocol-core RFC states the experimental release ships 'gated by the conformance suite' — the black-box protocol conformance suite being built in ably/realtime#8524, runnable against both realtime and ably-server. Wire ably-server up as a target: a documented way to run the suite against a local server, and a recorded baseline of which conformance behaviours pass, so the experimental release can state what is verified. Divergences found get filed as tasks.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The conformance suite runs against a local ably-server with a documented invocation
- [ ] #2 A baseline of passing/failing conformance behaviours is recorded in the repo
- [ ] #3 Failures are triaged into backlog tasks or documented residual gaps
<!-- AC:END -->
