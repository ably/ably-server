---
id: TASK-73
title: In-repo getting-started docs for the experimental release
status: To Do
assignee: []
created_date: '2026-07-09 11:07'
labels: []
dependencies: []
documentation:
  - 'https://ably.atlassian.net/wiki/spaces/product/pages/5171281935'
priority: medium
ordinal: 73000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PDR-090's experimental release requires 'in-repo docs for getting started etc'. README covers a Go quickstart, but the release audience is SDK users and FedRAMP reviewers: add getting-started docs covering install (binary/Docker/go install), connecting each mainstream SDK to a local endpoint (at minimum ably-js and ably-go option snippets), the auth model (API key, requestToken, JWT), a configuration reference for all flags/env vars, what is and is not supported relative to Ably cloud, and how to report feedback. Frame the release per the RFC: an experimental server whose protocol internals will progressively be replaced by code extracted from realtime (protocol-core).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Getting-started doc covers install, run, and connecting ably-js and ably-go with endpoint overrides
- [ ] #2 Configuration reference documents every flag and env var
- [ ] #3 Supported-functionality matrix vs Ably cloud, including explicit non-goals
- [ ] #4 Experimental status and feedback channel stated per PDR-090/RFC framing
<!-- AC:END -->
