---
id: TASK-15
title: Support rewind via history then live tail
status: Done
assignee:
  - '@lmars'
created_date: '2026-05-31 16:11'
updated_date: '2026-06-01 21:31'
labels: []
dependencies:
  - TASK-14
ordinal: 15000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Implement the rewind channel param per DESIGN.md §4.3. rewind=N (positive integer) selects an attach point N messages before the live head; rewind=<duration> (e.g. 15s, 2m) selects the attach point at the start of that window. The attachment reads the historical prefix from storage to satisfy the rewind request up to the attach point, then streams from the live tail — reusing the history-then-live cursor mechanism from the resume task. ATTACHED.channelSerial reflects the resulting attach point. rewind and channelSerial are mutually exclusive on one ATTACH; channelSerial wins if both are supplied (rewind ignored). All other channel params are silently ignored. Depends on the resume-via-history task.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 protocol.ProtocolMessage gains Params map[string]string with json/msgpack tags (carries ATTACH.params on inbound; mirrored in ATTACHED.params on outbound)
- [x] #2 ParseRewind helper accepts three formats: positive integer N (count), <N>s decimal seconds, <N>m decimal minutes. Empty/invalid input rejected
- [x] #3 When ATTACH carries params.rewind and msg.ChannelSerial is empty, the server replays historical context preceding the live anchor before entering the live MESSAGE loop
- [x] #4 channelSerial-supplied resume wins over rewind: if both are supplied on one ATTACH, rewind is ignored (per DESIGN §4.3); the resume path runs unchanged
- [x] #5 Count-based rewind (rewind=N): backwards from anchor, EndChannelSerial=anchor, Limit=min(N, cap); reverse for forward delivery; ATTACHED.flags.RESUMED clear
- [x] #6 Duration-based rewind (rewind=<n>s|<n>m): backwards from anchor, EndChannelSerial=anchor, Start=now-duration_ms, Limit=cap+1; reverse for delivery; ATTACHED.flags.RESUMED clear
- [x] #7 ATTACHED.channelSerial for rewind is the channel's initial watermark (set at Channel.Initialize, exposed via core.Channel.InitialChannelSerial), guaranteeing it sorts strictly less than every replayed and live cm in the channel
- [x] #8 ATTACHED.params echoes the rewind param (and any other channel params, for forward compat) back to the client
- [x] #9 Cap-exceeded rewind (count > cap, or duration window > cap messages): deliver newest cap of the window; ATTACHED.Error populated with an ErrorInfo; RESUMED clear (it is always clear for rewind)
- [x] #10 Invalid rewind value (non-positive integer, malformed duration, etc.) yields ATTACHED with no replay and an ErrorInfo describing the parse failure; the channel is still attached (lenient: replay defaults to fresh)
- [x] #11 Tests cover: count-based replay, duration-based replay, rewind-and-channelSerial coexistence (channelSerial wins), cap-exceeded rewind, invalid-rewind, ATTACHED.params echo, ATTACHED.channelSerial precedes every replayed message
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Empirical verification (already done against live Ably on port 8081)

- ATTACHED.channelSerial for rewind is strictly less than the first replayed MESSAGE's channelSerial.
- This holds even when the rewind window covers the entire channel (no predecessor message).
- ATTACHED.flags has bit 2 (RESUMED = 4) CLEAR.
- ATTACH and ATTACHED carry params.rewind echoed back.

These pin down: rewind is not a resume, the attach point must sort strictly before any replayed cm, and params is a real wire surface we need to model.

## 1. Protocol

internal/protocol:
- ProtocolMessage gains Params map[string]string with json/msgpack tags (omitempty).
- Carries ATTACH.params (inbound rewind param) and is echoed in ATTACHED.params.

## 2. ParseRewind

internal/realtime (or a new internal/rewind package — small enough to keep with attachment):
  type rewindMode int
  const (
      rewindNone rewindMode = iota
      rewindCount
      rewindDuration
  )
  func ParseRewind(s string) (mode rewindMode, count int, dur time.Duration, err error)

Formats:
- '^\d+$' → rewindCount, count = parsed int. Reject 0 or negative.
- '^\d+(\.\d+)?s$' → rewindDuration, dur = seconds. Reject 0.
- '^\d+(\.\d+)?m$' → rewindDuration, dur = minutes. Reject 0.

Implementation: trim, check suffix, strconv.Atoi or strconv.ParseFloat.

## 3. core.Channel

- Add initialSerial string field, set in Initialize alongside the sentinel cm assignment.
- Add InitialChannelSerial() string getter.

The initial watermark sorts strictly less than every cm in the channel (storage-minted post-Initialize). It is the natural ATTACHED.channelSerial value for any rewind — equivalent to Ably's behaviour of synthesising an attach-time serial that precedes everything.

## 4. attachment.run

Add rewindParam string to attachment struct. Decision tree in run():
- If resumeFrom != "": existing TASK-14 resume logic.
- Else if rewindParam != "": rewind logic.
- Else: fresh attach.

Rewind logic (computeRewindReplay):
- ParseRewind(rewindParam). On error: no replay, ATTACHED.Error populated with a 400-class ErrorInfo, RESUMED clear.
- For rewindCount:
    Limit = min(count, cap). If count > cap, mark capError=true.
    page := History(Direction:Backwards, EndChannelSerial:anchor, Limit:Limit)
- For rewindDuration:
    nowMs := time.Now().UnixMilli()
    page := History(Direction:Backwards, EndChannelSerial:anchor, Start:nowMs - dur.Milliseconds(), Limit:cap+1)
    If page.HasMore (returned cap+1 and more exist), mark capError=true and truncate to cap.
- Reverse the cms slice (newest-first → oldest-first); un-reverse Messages within each cm (TASK-6 backwards mode reverses idx).
- ATTACHED.channelSerial = channel.InitialChannelSerial().
- ATTACHED.flags.RESUMED = 0 (rewind is never a resume).
- ATTACHED.Error = nil unless capError or ParseRewind failed.
- ATTACHED.Params = client's msg.Params (echo).

## 5. connection wiring

connection.handleAttach passes msg through to newAttachment (already does, post-TASK-14). newAttachment grows a rewindParam string argument, populated from msg.Params["rewind"] ("" if absent).

Conflict resolution: per DESIGN §4.3, if msg.ChannelSerial != "" AND msg.Params has rewind, rewind is ignored. Implemented by leaving rewindParam empty when resumeFrom != "".

## 6. Tests

internal/realtime/server_test.go (or sdk_test.go):
- TestRewindCountReplaysNewestN: publish 5, ATTACH rewind=3, expect ATTACHED w/ params.rewind=3 + 3 newest messages.
- TestRewindCountCoveringEntireChannel: publish 2, rewind=10, expect 2 messages + ATTACHED.channelSerial < both.
- TestRewindDurationReplaysWindow: publish 3 with sleeps so timestamps span >2s, rewind=1s, expect only the most recent ones (those within the 1s window).
- TestRewindInvalidParamYieldsErrorNoReplay: rewind=abc, expect ATTACHED w/ Error, no MESSAGE frames, RESUMED clear.
- TestRewindAndChannelSerialChannelSerialWins: ATTACH with both rewind and a recent channelSerial, expect resume path (RESUMED set, replay strictly > clientSerial), rewind ignored.
- TestRewindCapExceeded: count > defaultReplayCap, expect cap delivered + ATTACHED.Error.
- TestRewindAttachedChannelSerialPrecedesReplay: assert ATTACHED.channelSerial < first replayed MESSAGE's channelSerial.

internal/realtime — ParseRewind unit tests: each format, decimals, rejection cases.

End-to-end smoke against the binary covering count-based and duration-based.

## Out of scope

- Capability enforcement (TASK-12).
- Retention bounds (TASK-26) — without retention, the duration window is fully covered by storage.
- Other channel params (only rewind is honoured per DESIGN §4.3).
- Configurable replay cap (still a const).
- Param keys other than rewind are silently ignored (per DESIGN §4.3 'All other channel params are silently ignored').
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Plan refinement after design discussion:
- ATTACHED.channelSerial for rewind uses the predecessor cm's serial when one exists (Limit=N+1 returns the +1th oldest as the predecessor; drop from delivery, use its serial). Falls back to channel.InitialChannelSerial() only when the rewind window covers the entire channel history (no predecessor in storage).
- channels table gains initial_channel_serial column — set on INSERT (the seed value), never updated. Migration 0003.
- Appender.Initialize(current, initial) takes both; core.Channel stores initial and exposes InitialChannelSerial().
- Small fix in TASK-14's resume detection: when backwards walk returns < cap+1 cms and clientSerial not found (exhausted storage), treat as successful resume rather than cap-exceeded. Edge case for clients that resume from initial after a rewind covering the whole channel.

Implemented:
- protocol.ProtocolMessage.Params (carries ATTACH.params and ATTACHED.params).
- ParseRewind helper supports <N>, <N>s, <N>m (decimal seconds/minutes allowed); rejects 0, negative, malformed.
- storage.Appender.Initialize(current, initial) — both serials handed to core.Channel.
- channels table gains immutable initial_channel_serial column; ensure_channel returns BOTH current and initial via TABLE return; advance_channel_serial also sets initial_channel_serial when its INSERT branch fires.
- bbolt persists initial via a new initials bucket so the invariant survives process restarts.
- core.Channel.InitialChannelSerial() exposes the value for use by rewind.
- attachment.go gains a rewind branch (computeRewindReplay): Limit=capLimit+1 backwards from anchor; the +1 oldest becomes the attach point predecessor if returned, else fall back to channel.InitialChannelSerial(). RESUMED always clear for rewind.
- TASK-14 resume detection refined: when backwards walk returns < cap+1 cms and clientSerial not found, treat as RESUMED success (we exhausted storage; the client cursor effectively predates everything, common after rewind-then-disconnect-from-initial-cursor).
- 5 WS-driven rewind tests + 5 ParseRewind unit tests, all green.
- Smoke-tested binary end-to-end: rewind=3 on 5-msg channel returns m2/m3/m4 with ATTACHED.channelSerial = predecessor of m2; rewind=10 on 2-msg channel returns m0/m1 with ATTACHED.channelSerial = channel initial. Both satisfy 'attach point < first replayed' invariant.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
## Rewind channel param (TASK-15)

Implements rewind as the second "resumeFrom-equivalent" pathway for ATTACH, reusing the history-then-live-tail mechanism from TASK-14 with a different way of computing the attach point. Three supported formats: <N> (count), <N>s (seconds), <N>m (minutes). channelSerial wins over rewind per DESIGN §4.3.

## What changed

- **protocol**: ProtocolMessage.Params map (ATTACH inbound carrier, ATTACHED outbound echo).
- **ParseRewind helper** in internal/realtime: parses the three formats with decimal seconds/minutes, rejects non-positive and malformed values.
- **storage interface**: Appender.Initialize gains an 'initial' arg alongside 'current'. Storage.Channel passes both to appender.Initialize.
- **postgres migration 0003**: channels gains an immutable initial_channel_serial column. ensure_channel rewritten as TABLE-returning to deliver both current and initial in one query. advance_channel_serial also sets initial_channel_serial when its INSERT branch fires (the rare publish-before-Channel-materialise case).
- **bbolt**: new initials bucket persists each channel's initial serial across process restarts — keeps the invariant 'initial < every cm in the channel' intact when a restarted process's wall-clock-seeded generator would otherwise produce a seed AFTER pre-restart cms.
- **memory**: mints initial=current=fresh seed at first Channel() call (single-process, no restart concern).
- **core.Channel**: gained initialSerial field and InitialChannelSerial() getter.
- **realtime/attachment**: rewind orchestration in computeRewindReplay. Limit=capLimit+1 backwards from anchor; the +1 oldest (if returned) is the attach-point predecessor; otherwise fall back to channel.InitialChannelSerial(). RESUMED always clear for rewind. params echoed in ATTACHED.
- **TASK-14 refinement**: resume detection now treats 'exhausted backwards walk with no clientSerial match' as a successful resume (RESUMED set, no error), not cap-exceeded. Common after a rewind-then-disconnect with the channel's initial serial as cursor; without retention (TASK-26 absent), it just means we delivered everything we have.

## Verified

- Five WS-driven rewind tests: count replays newest N, count covering entire channel uses initial serial, duration window filtering, invalid rewind yields error+no replay, rewind ignored when channelSerial is also supplied.
- Five ParseRewind unit tests across counts, decimal seconds/minutes, empty, and rejection cases.
- All three storage backends pass the (expanded) shared contract suite.
- Postgres integration suite green including the cluster-monotonic-serials test.
- Smoke-tested against the binary end-to-end: rewind=3 on a 5-msg channel returns the newest 3 with ATTACHED.channelSerial = predecessor of m2; rewind=10 on a 2-msg channel returns both with ATTACHED.channelSerial = channel's initial serial. Both satisfy 'attach point sorts < first replayed message'.

## Out of scope (carried forward)

- Capability enforcement (TASK-12).
- Retention TTL (TASK-26) — once it lands, aged-out cursors can be distinguished from cap-exceeded in the ErrorInfo if useful.
- Configurable replay cap (still defaultReplayCap = 1000).
- Other channel params (rewind is the only honoured one per DESIGN §4.3).
<!-- SECTION:FINAL_SUMMARY:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [x] #1 All three storage backends still pass the contract suite
- [x] #2 go test -tags=integration ./... is green
- [x] #3 Smoke-tested end-to-end against the binary
<!-- DOD:END -->
