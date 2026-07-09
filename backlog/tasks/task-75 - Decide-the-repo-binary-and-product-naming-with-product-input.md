---
id: TASK-75
title: 'Decide the repo, binary, and product naming (with product input)'
status: To Do
assignee: []
created_date: '2026-07-09 11:17'
labels: []
dependencies: []
documentation:
  - 'https://ably.atlassian.net/wiki/spaces/product/pages/5171281935'
priority: high
ordinal: 75000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Before the experimental release we need a product decision on naming. The repo is currently github.com/ably/server while the Go module and binary are ably-server — that mismatch needs resolving, and product should confirm whether the current name stays or we pick something else. The same decision covers how the binary is surfaced: advertised as a first-class standalone thing (ably-server), or encapsulated by the Ably CLI (e.g. an 'ably server' subcommand that runs the binary as a child process — PDR-090's Option C/dev-server framing leans this way, with installation via the CLI as part of onboarding). The outcome drives the module path, binary name, Docker image name, release artifacts (TASK-72), and docs (TASK-73), so it should land before those ship.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Product owners have decided the repo and product name (keep ably/server, or rename)
- [ ] #2 Decision recorded on whether the binary is first-class (ably-server) or CLI-encapsulated (ably server child process), and any CLI-integration follow-up tasks filed
- [ ] #3 Go module path, binary name, and Docker image name updated (or confirmed) to match the decision
- [ ] #4 Decision captured in backlog/decisions/
<!-- AC:END -->
