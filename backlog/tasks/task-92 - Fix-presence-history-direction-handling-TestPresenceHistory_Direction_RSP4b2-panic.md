---
id: TASK-92
title: >-
  Fix presence history direction handling (TestPresenceHistory_Direction_RSP4b2
  panic)
status: Done
assignee:
  - '@claude'
created_date: '2026-07-10 14:31'
updated_date: '2026-07-12 00:03'
labels:
  - rest
  - presence
  - compat
dependencies: []
priority: medium
ordinal: 92000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-10.md gap 3: presence is otherwise green after the fixtures work, but TestPresenceHistory_Direction_RSP4b2 panics — newly visible now that --fixtures seeds members the test can page over (it did not fail in the TASK-80 verification runs on 07-09, and a bare direction=forwards query on an empty channel returns 200 at HEAD, so the failure needs seeded data and likely pagination to reproduce). Diagnose what the SDK receives for a direction-qualified presence-history read over the fixture data (wrong ordering, missing/odd Link header, or a response shape the SDK nil-derefs on) and fix HandlePresenceHistory to match message history's direction semantics. Reproduce/verify via scripts/ably-server-compat.sh -r '^TestPresenceHistory_Direction_RSP4b2$' (the script seeds fixtures by default; ably-go checkout is read-only).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Root cause of the panic recorded (what the SDK received)
- [x] #2 TestPresenceHistory_Direction_RSP4b2 passes against a local server
- [x] #3 Unit test in internal/rest pins direction-qualified presence-history ordering and pagination
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Reproduce TestPresenceHistory_Direction_RSP4b2 against a --fixtures-seeded local server; capture the exact panic + what the SDK received. 2. Read the ably-go test to see what shape/ordering/Link headers it expects for direction-qualified presence history. 3. Compare HandlePresenceHistory to HandleHistory direction semantics; fix presence history to honour direction (and pagination) the same way. 4. Add a unit test in internal/rest pinning direction-qualified presence-history ordering + pagination. 5. Re-run scripts/ably-server-compat.sh -r 'Presence'.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
ROOT CAUSE — NOT a server bug; a client-side channel-name-collision harness artifact (TASK-90 class).

Repro/verification:
- Fresh server, isolated run of the test: PASS both directions in 0.02s.
- scripts/ably-server-compat.sh -r '^TestPresenceHistory_Direction_RSP4b2$' (fresh server, only this test — the command TASK-92 prescribes): PASS (1 PASS / 0 FAIL / 0 PANIC).
- Full 'Presence' sweep (shared server, batch order): PANIC on Direction only (8 PASS / 0 FAIL / 1 PANIC).

Why it 'panics' only in the batch: the harness runs each test in its own 'go test' process against ONE shared server. ably-go's ablytest.ChannelName("persisted:test") uses a per-PROCESS counter that resets each process, and against the local server NewSandbox is stubbed (ABLY_LOCAL_KEY) so every process shares the same app — so channel names collide across processes. TestPresenceHistory_RSP4_RSP4b3 (enumerated first) writes fixtures to persisted:test-1/-2/-3 via realtime; TestPresenceHistory_Direction_RSP4b2 then reuses persisted:test-1/-2 and enters ITS 19 fixtures onto the already-populated presence stream. Its assertion demands an EXACT-match page of exactly its 19 fixtures (limit=len(expected)); the polluted stream never matches, ablytest.WaitFor retries the full 30s per subtest, 2 subtests => 62s > the 60s per-test timeout => 'panic: test timed out' dump => harness classifies PANIC. Reproduced deterministically by running RSP4b3 then Direction against one server (62.05s FAIL/timeout).

Server behaviour verified CORRECT: direction-qualified presence history and its rel=next cursor already mirror message history (parseHistoryQuery honours direction; memory backend applies direction+cursor symmetrically for KindPresence; writeHistoryLinks/lastMessageSerial emit the presence serial cursor). New unit test internal/rest.TestPresenceHistoryDirectionPagination walks 5 members at limit=2 forwards (a..e) and backwards (e..a) across 3 pages via the opaque next link and passes — pinning ordering + pagination. No SDK nil-deref/response-shape issue found; the report's 'PANIC' is the WaitFor timeout dump, consistent with its own note that some panics are hangs the per-test timeout converts.

No server code changed: no defect exists. The full-'Presence'-batch PANIC is an ably-go local-harness app-isolation limitation (ChannelName counter + shared app), not an ably-server gap — same category as the report's host-fallback harness artifacts. Future compat reports should classify TestPresenceHistory_Direction_RSP4b2 as a harness channel-collision artifact (passes in isolation), not a server regression.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Diagnosed: not a server bug. TestPresenceHistory_Direction_RSP4b2 passes against a local server both in an isolated 'go test' run (0.02s) and via the task's prescribed command scripts/ably-server-compat.sh -r '^TestPresenceHistory_Direction_RSP4b2$' (fresh server). The full-'Presence'-batch PANIC is a channel-name-collision artifact: ably-go's per-process ChannelName counter collides against the shared local app, so TestPresenceHistory_RSP4_RSP4b3 (runs first) pollutes persisted:test-1/2, and Direction's exact-match expectation then never matches, WaitFor stacks 2x30s past the 60s per-test timeout -> panic dump. Server direction+cursor semantics for presence already mirror message history and are verified correct by a new unit test (internal/rest.TestPresenceHistoryDirectionPagination, forwards a..e and backwards e..a across 3 limit=2 pages). AC#2 satisfied via the task's own verify command; the batch PANIC is a client-side harness limitation, recorded for future report classification.
<!-- SECTION:FINAL_SUMMARY:END -->
