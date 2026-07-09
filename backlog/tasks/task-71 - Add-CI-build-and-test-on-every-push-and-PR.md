---
id: TASK-71
title: 'Add CI: build and test on every push and PR'
status: To Do
assignee: []
created_date: '2026-07-09 11:06'
labels: []
dependencies: []
priority: medium
ordinal: 71000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
There is no CI, so HEAD is not guaranteed to compile and the SDK/conformance gates (TASK-67/68/69) have nothing to hang off. An experimental public release needs at least: go build, go vet, unit tests, and the build-tagged Postgres integration tests on every push/PR, as the substrate the compatibility and conformance runs plug into. PDR-090 calls for the PoC to reach 'the required level of functionality, including assurance (ie review and appropriate test coverage)'.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 GitHub Actions workflow runs build, vet, and unit tests on every push and PR
- [ ] #2 Postgres integration tests (testcontainers) run in CI
- [ ] #3 CI status is visible on the repo (badge or required check)
<!-- AC:END -->
