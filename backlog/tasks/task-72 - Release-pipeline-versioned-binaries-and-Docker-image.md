---
id: TASK-72
title: 'Release pipeline: versioned binaries and Docker image'
status: To Do
assignee: []
created_date: '2026-07-09 11:06'
labels: []
dependencies: []
priority: medium
ordinal: 72000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PDR-090's experimental phase ends with the server 'made available as an experimental local development server for customer/community feedback' — which needs a distribution: tagged, versioned GitHub releases with prebuilt single binaries for the mainstream platforms (linux/darwin, amd64/arm64), a published Docker image, and the version/commit stamped into the binary (visible via a --version flag and ideally the CONNECTED/serverId surface). Release notes must label the release experimental per the PDR framing.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Tagging a version produces a GitHub release with binaries for linux/darwin on amd64/arm64
- [ ] #2 A Docker image is published per release
- [ ] #3 ably-server --version reports version and commit
- [ ] #4 Release notes template labels the build experimental and links feedback channels
<!-- AC:END -->
