---
id: TASK-68
title: Run the ably-js test suite against ably-server and triage the gaps
status: To Do
assignee: []
created_date: '2026-07-09 11:06'
updated_date: '2026-07-12 10:39'
labels: []
dependencies:
  - TASK-95
documentation:
  - 'https://ably.atlassian.net/wiki/spaces/product/pages/5171281935'
priority: high
ordinal: 68000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PDR-090's experimental release explicitly requires 'full client SDK (eg ably-js) test coverage (for the functional scope)'. Scouting (2026-07-12) established: the node suite (~22 realtime + 19 rest spec files, fanned out across transports and json/msgpack) is env-routable with ABLY_ENDPOINT=localhost ABLY_USE_TLS=false ABLY_PORT=<port> — fallback hosts self-disable and test-app provisioning follows the same endpoint via POST /apps (test/common/modules/testapp_manager.js). Strategy agreed with Lewis: run it against the TASK-95 sandbox provisioner. One small upstreamable harness change in ably-js (branch, like ably-go's server-testing): testapp_manager/client_module honour endpoint/port/tls fields on the app-creation response so clients route at the provisioned child server. Build first (grunt build:node build:push build:liveobjects, or npm run test:node which does both). Then run the full node suite, triage every failure into: existing backlog task, new task, documented artifact, or documented non-goal (expected permanent reds: push, stats-data assertions, LiveObjects), and produce a compatibility report in this repo.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 ably-js test suite runs against a local ably-server via a committed, documented harness
- [ ] #2 A compatibility report maps every failure to a backlog task or documented non-goal
- [ ] #3 New tasks filed for in-scope gaps the run uncovers
<!-- AC:END -->
