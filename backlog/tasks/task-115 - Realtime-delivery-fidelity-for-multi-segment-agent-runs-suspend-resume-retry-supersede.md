---
id: TASK-115
title: >-
  Realtime delivery fidelity for multi-segment agent runs (suspend/resume +
  retry-supersede)
status: Done
assignee:
  - '@claude'
created_date: '2026-07-12 17:36'
updated_date: '2026-07-12 20:43'
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

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Instrument (server delivery slog.Debug in attachment.forward; witness logLevel 4; child --log-level debug) and run the DETERMINISTIC durable test ONCE unfixed to produce a correlated trace. 2. Fix the proven cause in storage.MergeVersion (carry name forward on the append delta) plus the reference-faithful top-level Message.Timestamp stamping. 3. Update DESIGN.md §8/§13.2/§13.3. 4. Unit tests (storagetest across backends: delta carries name/extras/timestamp; wire timestamp on create/update/append/history). 5. Bounded verify: deterministic 3x, full AIT once, flaky suspend/resume once, ably-js regression once. 6. Clean up instrumentation.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Hypothesis from Lewis (2026-07-12): the divergence may be extras handling on updates/appends — TASK-105 chose 'append delta carries the MERGED extras', but AIT's decoder routes every message on extras.ai; if the reference delivers the append operation's OWN extras verbatim on the delta, our frames would be semantically invisible to the SDK while looking delivered. Verify against the reference (coordinator.buildUpdateMessage) AND empirically (raw frame capture, cloud vs server, diff extras/action/serials/timestamp).

Reference reading (coordinator.buildUpdateMessage, verified 2026-07-12): supplied extras replace whole-object, absent extras carry forward; the append DELTA is cloned AFTER carry-forward population but BEFORE data concatenation (delta carries the operation's own-or-carried-forward extras, not a merged aggregate); top-level Timestamp of every update/append delivery is overwritten with the ORIGINAL message's timestamp (operation time only in version.timestamp); annotations+internal carry forward unconditionally.

QUARANTINED PRIOR WORK (git stash: 'QUARANTINE: unauthorized TASK-115 work...'): an agent re-woken by a stale watcher implemented, unauthorized and unverified: (a) an ACK-held-behind-self-echo mechanism + a DESIGN.md §2.1 claim that Ably orders ACK after echo — claim has NO evidence, contradicts TASK-20 ACK-on-commit semantics, and must NOT be adopted without reference-code proof and a cloud frame capture; (b) top-level Message.Timestamp stamping at create + carry-forward through versions — this half MATCHES the verified reference behaviour and is plausibly the real TASK-115 root cause (an absent timestamp is omitted on the wire; SDK folds that read timestamps see undefined). Whoever picks this task up: start from the hypothesis + reference reading, treat the stash as unreviewed input only, and prove any fix with the wire-capture A/B before adopting.

## Correlated-trace verdict (2026-07-12, DETERMINISTIC durable test, instrumented single run)

Ran 'supersedes an abandoned partial step' ONCE against the UNFIXED server (HEAD e9c2040) with: server-side slog.Debug in attachment.forward logging every delivered message's action/serial/name/hasExtras/ts/dataLen; child at --log-level debug; witness SDK at logLevel 4. Test failed deterministically: 'observer timed out waiting for: attempt-1 output'.

Correlated trace (server delivered vs witness received) — the append delta for the dead partial:
  SERVER: action=append serial=...228132:000 name="" nameEmpty=true hasExtras=true (step-id:wf-step-X present) ts=0 dataLen=19 ('DEAD partial answer')
  WITNESS (wire): [WireMessage; serial=...228132:000; action=5; data=DEAD partial answer; extras={ai:{...step-id:wf-step-X...}}]  -- NO name field on the frame.
The preceding create frames carry name=ai-output but dataLen=0. So the ONLY frame carrying 'DEAD partial answer' has no name; the witness predicate requires name==='ai-output' AND data includes 'DEAD partial answer' -> never satisfied -> timeout.

ROOT CAUSE: the append DELTA delivered to a caught-up subscriber omits the message name. The create carried name=ai-output; the AIT encoder's appendStream (encoder.ts) omits name on the append (relies on carry-forward); server MergeVersion built the delta fresh WITHOUT carrying name forward (HEAD set delta.Name only when the append supplied its own).

VERDICT ON BOTH HYPOTHESES:
- Lewis's EXTRAS hypothesis: REFUTED. The append supplies its own extras and the server delivers them verbatim on the delta (hasExtras=true; step-id:wf-step-X present on the wire). Extras were never the missing field.
- Stash's TIMESTAMP hypothesis: NOT the deterministic cause. ts=0 (top-level Message.Timestamp absent, omitempty) IS observed on every delivery, but the witness predicate never reads timestamp; the timeout is fully explained by the missing name. Top-level timestamp absence is a real reference-fidelity gap (buildUpdateMessage stamps it) but does not cause THIS deterministic failure.

FIX: carry the message name forward onto the append delta (the load-bearing fix), AND stamp top-level Message.Timestamp at create + carry it through versions/delta (reference-faithful per the verified buildUpdateMessage reading; operation time stays in version.timestamp). No ACK-after-echo, no ACK delaying.

## Fix + verification (2026-07-12)

FIX (fresh, reference-faithful; stash NOT applied):
- storage.MergeVersion: the append delta now carries the message NAME forward (v.Name = append-supplied-or-carried-forward), built after identity carry-forward — the load-bearing fix for the deterministic durable failure.
- storage.StampCreateVersion: stamps top-level Message.Timestamp = create time; carried forward unchanged through every version and onto the delta (v := *current copy), matching buildUpdateMessage (top-level = original create time; operation time stays in version.Timestamp). No ACK-after-echo, no ACK delaying, no DESIGN §2.1 'ACK ordered after echo' claim.
- DESIGN.md §8 (new Message.timestamp bullet), §13.2 (top-level timestamp is the original create time), §13.3 (delta repeats name/extras/create-timestamp).
- Tests: storagetest (all backends) CreateStampsTopLevelTimestamp, MutateCarriesTopLevelCreateTimestampForward, MutateAppendDeltaCarriesIdentityForward; realtime wire test TestAppendDeltaCarriesIdentityToCaughtUpSubscriber (named create + name-less append -> delta carries name/extras/timestamp).

GATES (all green): go build ./...; go vet ./...; go vet -tags=integration ./...; go test -count=1 ./...; go test -race ./internal/realtime/; go test -tags=integration -race ./internal/storage/postgres/... (Docker/testcontainers).

VERIFICATION:
- Deterministic durable 'supersedes an abandoned partial step': 3/3 PASS (was 0/2).
- Full AIT integration suite: 61/61 PASS (was 59/61). The flaky agent suspend/resume test passed within this full run.
- Flaky 'run.messages spans a suspend/resume run' ISOLATED confirmation run: FAILED (timeout ~10s in awaitRunComplete) — i.e. it passes in the full-suite context but fails standalone, matching the documented ~1/3 server flakiness. My change does NOT deterministically resolve the flaky agent case; per the hard rule I did not loop or re-run it. This is the self-echo/ordering symptom the REJECTED quarantine targeted; it needs separate triage (a fresh task) and must NOT be fixed with ACK-after-echo.
- ably-js regression (no regressions vs TASK-97/105): updates-deletes 3 passing/3 failing (append residual = version.clientId undefined, TASK-109); annotations 0 passing/2 failing (TASK-108 REST); message.test.js --grep comet --invert 41 passing/1 failing (subscribes-to-filtered-channel, pre-existing).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Root-caused via a single instrumented deterministic run: the streamed-append delta delivered to a caught-up subscriber dropped the message name (the AIT encoder omits name on appends, relying on carry-forward; MergeVersion built the delta without it), so the witness filtering on name==='ai-output' never saw the append. Fix carries the name forward onto the delta and stamps the top-level Message.Timestamp at create (carried through versions/delta), matching the reference buildUpdateMessage; DESIGN §8/§13.2/§13.3 updated; unit tests added across storage backends and at the realtime wire. Verdict on hypotheses: extras REFUTED (delivered verbatim), timestamp not the deterministic cause (fixed anyway for reference fidelity). Deterministic durable test 3/3; full AIT suite 61/61; all Go gates green (incl postgres integration race); no ably-js regressions. The flaky agent suspend/resume test passes in the full suite but fails a standalone confirmation run — pre-existing self-echo/ordering flakiness this change does not deterministically resolve; needs a separate task and must not use ACK-after-echo.
<!-- SECTION:FINAL_SUMMARY:END -->
