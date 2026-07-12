---
id: TASK-68
title: Run the ably-js test suite against ably-server and triage the gaps
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 11:06'
updated_date: '2026-07-12 14:01'
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
- [x] #1 ably-js test suite runs against a local ably-server via a committed, documented harness
- [x] #2 A compatibility report maps every failure to a backlog task or documented non-goal
- [x] #3 New tasks filed for in-scope gaps the run uncovers
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Build ably-server + ably-sandbox binaries to /tmp/ably-bin.
2. Create ably-js worktree at ../ably-js-server-testing on new branch server-testing from main; verify submodule; npm install.
3. Patch testapp_manager.js + client_module.js (+shared_helper if needed) so per-app endpoint/port/tls from the provisioner response route clients at the child server; keep provisioning host for POST /apps, DELETE /apps, POST /stats. Commit on server-testing.
4. Run provisioner on :9080 with --server-bin, logging to file.
5. grunt build:node build:push build:liveobjects once. Sanity-check test/rest/time.test.js with ABLY_ENDPOINT=localhost ABLY_PORT=9080 ABLY_USE_TLS=false.
6. Run full node suite per-file with hard timeout (~420s), capture per-file logs, tally pass/fail.
7. Triage failures into: existing task/non-goal (push, stats, LiveObjects, delta TASK-34, channel-status TASK-35), harness artifacts, genuine gaps.
8. File one backlog task per coherent genuine gap (label compat,ably-js).
9. Write COMPAT_REPORT_ABLY_JS_2026-07-12.md (untracked). Kill provisioner + children.
10. Update task notes/final-summary, check ACs, mark Done; commit backlog only.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Run complete 2026-07-12. Harness: ably-js worktree at /Users/lewis/src/github.com/ably/ably-js-server-testing, branch server-testing, commit 0e095807 (single upstreamable commit on main c90f1c38): testapp_manager.js threads the provisioner's endpoint/port/tls from the POST /apps response onto the test app; client_module.js routes clients at that per-app child server before the caller's options mixin (explicit overrides still win); deletion/stats fixtures still hit the provisioning host; stock behaviour when fields absent. Submodule initialised in the worktree.

Run: server @156f5dc, ably-sandbox on :9080 with --server-bin; 44 spec files, one mocha process each, --timeout 15000, 600s per-file backstop; channel/message/reauth re-run with --grep comet --invert (comet variants counted under the TASK-99 finding). Totals: 370 pass / 210 fail / 36 pending (realtime 262/132/32, rest 94/78/4, unit 14/0/0); channel.test.js partial (27/29, capped at 600s even without comet).

Headline: ProtocolMessage.MsgSerial omitempty drops msgSerial:0 from the first ACK/NACK per connection; ably-js never completes its first awaited publish/enter — ~50 of the 210 failures cascade from this one bug (TASK-97). ably-go decodes the missing field as 0, which is why its suite never caught it.

Triage: every failing area mapped in COMPAT_REPORT_ABLY_JS_2026-07-12.md — (a) non-goals/existing tasks: push 21, stats 9, LiveObjects, status TASK-35, delta TASK-34 (blocked behind 97), reauth capability flows TASK-88, recover non-goal, resume TASK-85; (b) artifacts: tls-default asserts, echo.ably.io-dials-localhost JWT tests; (c) 14 new tasks filed TASK-97..110 (ACK msgSerial, in-band auth ERROR, comet scoping, requestToken fidelity, HEARTBEAT echo, channel-name validation, ATTACHED params echo, binary transcoding, extras, fromSerial, Link ./-prefix+msgpack /time, REST publish identity, mutation metadata shape, batch API scoping). Server-side mechanics verified by code inspection and live curl for the major ones. Logs/tallies: /tmp/ablyjs-run/.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Ran the full ably-js node suite (44 files) against ably-server via the TASK-95 sandbox provisioner. Committed a minimal, upstreamable harness change on the ably-js server-testing branch (worktree ably-js-server-testing, commit 0e095807): clients route at the per-app child server advertised in the provisioner's POST /apps response. Result: 370 pass / 210 fail / 36 pending. Every failure classified in COMPAT_REPORT_ABLY_JS_2026-07-12.md: ~50 cascade from one wire bug (ACK/NACK omit msgSerial:0 via omitempty — TASK-97), ~70 are comet/push/stats/LiveObjects/status/delta expected reds, the rest map to 14 newly-filed tasks (TASK-97..110) spanning auth-failure semantics, requestToken fidelity, HEARTBEAT echo, channel-name validation, ATTACHED params echo, binary transcoding, extras, history fromSerial, REST envelope conformance, publish identity, mutation metadata, and two scoping decisions (comet, batch API). Recommended order: TASK-97 first (flips ~50 tests), then TASK-100+98, then re-sweep.
<!-- SECTION:FINAL_SUMMARY:END -->
