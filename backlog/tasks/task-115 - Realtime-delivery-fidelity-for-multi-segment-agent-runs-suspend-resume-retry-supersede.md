---
id: TASK-115
title: >-
  Realtime delivery fidelity for multi-segment agent runs (suspend/resume +
  retry-supersede)
status: To Do
assignee: []
created_date: '2026-07-12 17:36'
updated_date: '2026-07-12 18:36'
labels:
  - compat
  - ait
dependencies: []
priority: medium
ordinal: 115000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The two residual AIT integration failures once the capability wall (TASK-114) is removed. Both PASS against the cloud sandbox but fail/flake against ably-server, so both are genuine server gaps, not test artifacts. No server-side ERROR/WARN is logged in either case.

## Failing tests + rates (per-app provisioner vs cloud sandbox, keys[0])
1. `agent-session.integration.test.ts > run.messages spans a suspend/resume run: original input then all output` — SERVER 1/3 pass (flaky), CLOUD 3/3 pass. Times out in `awaitRunComplete`: the agent (publisher+subscriber on one channel) intermittently never sees its own `ai-run-end` fold onto its Tree after a suspend→resume under the same runId.
2. `durable-cross-process.integration.test.ts > supersedes an abandoned partial step from a fresh process (same stepId, later serial)…` — SERVER 0/2 pass (deterministic), CLOUD 3/3 pass. A separate witness subscriber times out waiting for `attempt-1 output`: the abandoned attempt's in-progress streamed append (`ai-output` for stepId wf-step-X, data contains 'DEAD partial answer', from a stream that emits one text-delta then never closes) is never delivered live.

## Shared theme
Both exercise multi-segment runs where a run/step is re-entered under the same id with a later serial (resume of a suspended run; retry that supersedes an abandoned partial), and a live subscriber must observe the complete realtime sequence of appends + lifecycle events. Symptom is a missed/undelivered realtime message (lifecycle event or an in-progress append), not a rejection. Likely one root cause in the realtime fan-out / append-delivery / serial-ordering path; confirm during fix whether the deterministic (durable) and flaky (agent) cases share it.

## Evidence
- 59/61 of the AIT integration suite passes against ably-server once keys[0] sidesteps TASK-114; these two are the only residuals.
- Both pass 3/3 on cloud sandbox with the same harness and key.
- No child-server ERROR/WARN logged during the failing runs.

Demo relevance: resume continuity and crash/retry-supersede are core durable-workflow demo flows. Do not fix in TASK-96 (run+triage only).
<!-- SECTION:DESCRIPTION:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Hypothesis from Lewis (2026-07-12): the divergence may be extras handling on updates/appends — TASK-105 chose 'append delta carries the MERGED extras', but AIT's decoder routes every message on extras.ai; if the reference delivers the append operation's OWN extras verbatim on the delta, our frames would be semantically invisible to the SDK while looking delivered. Verify against the reference (coordinator.buildUpdateMessage) AND empirically (raw frame capture, cloud vs server, diff extras/action/serials/timestamp).

Reference reading (coordinator.buildUpdateMessage, verified 2026-07-12): supplied extras replace whole-object, absent extras carry forward; the append DELTA is cloned AFTER carry-forward population but BEFORE data concatenation (delta carries the operation's own-or-carried-forward extras, not a merged aggregate); top-level Timestamp of every update/append delivery is overwritten with the ORIGINAL message's timestamp (operation time only in version.timestamp); annotations+internal carry forward unconditionally.

QUARANTINED PRIOR WORK (git stash: 'QUARANTINE: unauthorized TASK-115 work...'): an agent re-woken by a stale watcher implemented, unauthorized and unverified: (a) an ACK-held-behind-self-echo mechanism + a DESIGN.md §2.1 claim that Ably orders ACK after echo — claim has NO evidence, contradicts TASK-20 ACK-on-commit semantics, and must NOT be adopted without reference-code proof and a cloud frame capture; (b) top-level Message.Timestamp stamping at create + carry-forward through versions — this half MATCHES the verified reference behaviour and is plausibly the real TASK-115 root cause (an absent timestamp is omitted on the wire; SDK folds that read timestamps see undefined). Whoever picks this task up: start from the hypothesis + reference reading, treat the stash as unreviewed input only, and prove any fix with the wire-capture A/B before adopting.
<!-- SECTION:NOTES:END -->
