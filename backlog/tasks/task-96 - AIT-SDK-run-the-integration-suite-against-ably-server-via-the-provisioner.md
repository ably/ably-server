---
id: TASK-96
title: 'AIT SDK: run the integration suite against ably-server via the provisioner'
status: Done
assignee:
  - '@claude'
created_date: '2026-07-12 10:39'
updated_date: '2026-07-12 17:37'
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
- [x] #1 The AIT integration suite runs against a provisioner-booted ably-server with env-only configuration (helper patch committed on a branch in ably-ai-transport-js)
- [x] #2 A compatibility report maps every failure to a backlog task or documented artifact
- [x] #3 New tasks filed for in-scope server gaps
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Create AIT worktree on branch server-testing; submodule init; pnpm install (Node 22). 2. Patch test helpers: configurable VITE_ABLY_PROVISION_URL, thread endpoint/port/tls from /apps response into realtime-client; keep keys[5]. Commit on worktree. 3. Build server binaries; run ably-sandbox on :9080. 4. Run all 5 integration specs per-file; triage every failure (cloud-sandbox A/B to separate server gaps from artifacts). 5. File one backlog task per coherent server gap. 6. Write COMPAT_REPORT_AIT_2026-07-12.md with a demo-readiness verdict.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Per Lewis (2026-07-12): the goal of this task is THE central question for the experimental release — can the server be used with the AIT SDK and power demos. Everything else is secondary. Run after TASK-105 (extras.ai is a hard AIT dependency).

Run (2026-07-12, server e9c2040, AIT worktree 44f04093 on 2a7734dc):
- Provisioner /tmp/ably-bin/ably-sandbox --server-bin /tmp/ably-bin/ably-server --listen :9080; per-file vitest with backstop timeout.
- With committed keys[5] (cap {"[*]*":["*"]}): 0/61 — every test 40160 insufficient capability at attach/publish. THE blocker.
- With experimental keys[0] (uncommitted, to see the surface): 59/61. Per file: agent 16/17, client 24/24, durable 6/7, codec 11/11, errprop 2/2.
- Both residual failures (agent run.messages suspend/resume [flaky, srv 1/3]; durable supersede abandoned partial [srv 0/2]) PASS 3/3 on cloud sandbox → genuine server gaps, not artifacts. No child ERROR/WARN logged.
- Root cause of the blocker: internal/auth/capability.go matchResource treats [*]* as a literal (splits on ':', only bare '*' is a wildcard); ably-js escaped it by using keys[0]=undefined→{"*":["*"]}.
- Filed TASK-114 (high: [qualifier]resource wildcard) and TASK-115 (medium: multi-segment realtime delivery fidelity). Report: COMPAT_REPORT_AIT_2026-07-12.md.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Ran the AIT integration suite (61 tests, 5 specs) against a provisioner-booted ably-server. Committed an env-only harness patch on the server-testing branch (configurable VITE_ABLY_PROVISION_URL + route clients at the per-app child from the /apps endpoint/port/tls; keeps keys[5]). Found one hard blocker — the capability matcher can't match the sandbox all-access key {"[*]*":["*"]}, killing all 61 tests (TASK-114, high). Behind that wall the suite is 59/61 green: streaming, appends, history hydration, resume, cancel all work. The two residuals (suspend/resume run.messages; retry-supersede) pass on cloud but fail on server → TASK-115 (medium). Verdict: land TASK-114 and the AIT SDK powers demos today. Full write-up in COMPAT_REPORT_AIT_2026-07-12.md.
<!-- SECTION:FINAL_SUMMARY:END -->
