---
id: TASK-116
title: Intermittent missed delivery on suspend/resume re-entry (agent-session flake)
status: To Do
assignee: []
created_date: '2026-07-12 20:44'
labels:
  - realtime
  - compat
  - ait
dependencies: []
priority: medium
ordinal: 116000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The residual half of TASK-115. After the append-delta name/timestamp fix (4900109, AIT suite 61/61), the agent-session test 'run.messages spans a suspend/resume run' still fails intermittently (~1 in 3) in isolation: the agent (publisher+subscriber on one channel) sometimes never observes its own ai-run-end after resuming a suspended run under the same runId. The rejected quarantined attempt targeted this with an ACK-held-behind-self-echo mechanism — that approach is REJECTED unless the ordering guarantee is first proven from the reference code, with the proof recorded here. Diagnosis rules (per Lewis, recorded in memory + TASK-115 notes): instrument first (server debug logs + client logLevel 4 + raw frame capture), no reproduction loops — each run must carry new instrumentation; the single instrumented-run observations from TASK-115's confirmation run are in that task's notes as a starting point. Plausible directions to examine with evidence: self-echo delivery vs ACK interleaving as observed (not assumed), attachment seen-tracking under same-id re-entry, publish-worker/attachment-cursor races on the publisher's own connection.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Root cause established from an instrumented failing run (trace recorded in notes), not from repetition
- [ ] #2 The suspend/resume test passes its confirmation run after the fix; full AIT suite stays 61/61
- [ ] #3 Any ordering guarantee relied upon is proven from the reference and recorded
<!-- AC:END -->
