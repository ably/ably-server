---
id: TASK-96
title: 'AIT SDK: run the integration suite against ably-server via the provisioner'
status: To Do
assignee: []
created_date: '2026-07-12 10:39'
labels:
  - compat
  - ait
dependencies:
  - TASK-95
priority: high
ordinal: 96000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
ably-ai-transport-js's integration suite (~61 tests across 5 specs) stresses exactly ably-server's newest surface: streamed appends on mutable channels, update/delete, history pagination, resume continuity, attach modes — no presence/annotations/LiveObjects/push. Its CI provisions against the cloud sandbox (POST test-app-setup post_apps to sandbox-rest.ably.io, uses keys[5]); the local path hardcodes local-rest.ably.io:8081 and skips provisioning. Patch the test helpers (their repo, on a branch, upstreamable — test/helper/test-setup.ts + environment.ts + realtime-client.ts): make the provisioning URL configurable and take the client endpoint/port/tls from the POST /apps response (falling back to current behaviour), so the suite runs against the TASK-95 provisioner with env only. Prereqs: git submodule update --init (ably-common is not checked out), pnpm install, Node >= 22. Then run pnpm test:integration against the provisioner, triage every failure (server gap → backlog task; SDK/test artifact → documented with evidence), and produce a compatibility report in this repo.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The AIT integration suite runs against a provisioner-booted ably-server with env-only configuration (helper patch committed on a branch in ably-ai-transport-js)
- [ ] #2 A compatibility report maps every failure to a backlog task or documented artifact
- [ ] #3 New tasks filed for in-scope server gaps
<!-- AC:END -->
