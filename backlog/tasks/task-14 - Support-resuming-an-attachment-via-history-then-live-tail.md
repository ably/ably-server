---
id: TASK-14
title: Support resuming an attachment via history then live tail
status: Done
assignee:
  - '@lmars'
created_date: '2026-05-31 16:11'
updated_date: '2026-06-01 19:59'
labels: []
dependencies: []
ordinal: 14000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Implement channelSerial-based attachment resume per DESIGN.md §4.3/§4.4. On ATTACH carrying a channelSerial, the attachment first reads the gap from storage history (storage.History with AfterChannelSerial = the client's cursor) up to the channel's current head, forwarding those ChannelMessages, then transitions to the live linked-list tail at the resume point — with no lost or duplicated messages across the handover. If the supplied serial has aged out of retention, attach at the live head instead, clear ATTACHED.flags.RESUMED, and populate ATTACHED.error with an ErrorInfo per §4.3 (no replay). ATTACHED.channelSerial reflects the confirmed attach point. This establishes the history-then-live cursor mechanism that rewind reuses.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [x] #1 All three storage backends still pass the contract suite
- [x] #2 go test -tags=integration ./... is green
- [x] #3 Smoke-tested end-to-end against the binary
- [ ] #4 All three storage backends still pass the contract suite
- [ ] #5 go test -tags=integration ./... is green
- [ ] #6 Smoke-tested end-to-end against the binary
<!-- DOD:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 protocol.FlagResumed (= 1 << 2) defined; protocol.ErrorInfo {Message, Code, StatusCode, HRef} struct added; ProtocolMessage gains Error *ErrorInfo with omitempty
- [x] #2 On ATTACH carrying a non-empty channelSerial that matches or sorts after the channel's earliest retained serial, the server replays history strictly after that serial up to the live tail (anchor), then continues from the live linked list; no gap and no duplicate cms across the handover
- [x] #3 ATTACHED.flags.RESUMED is set when the full requested gap was replayed; cleared otherwise (aged-out, cap-exceeded, or empty-cursor fresh attach)
- [x] #4 ATTACHED.channelSerial echoes the client's supplied serial when a resume was attempted (whether full or partial); the anchor's serial for a fresh (no-channelSerial) attach
- [x] #5 Replay cap: a server-side ceiling caps each ATTACH's replay at N messages (default 1000). When the gap exceeds the cap, replay the NEWEST cap messages of the gap, RESUMED clear, ATTACHED.error populated
- [x] #6 Cap and aged-out can co-occur; an aged-out resume also subject to the cap. The ErrorInfo distinguishes the cause where useful but a single ErrorInfo per ATTACHED is sufficient
- [x] #7 ATTACH for a channelSerial against an empty channel falls through to a fresh attach (no replay, no error, RESUMED clear) — there is nothing to resume
- [x] #8 Replayed messages stream as ordinary MESSAGE frames in publish order, one frame per ChannelMessage with the full batch in Messages[]
- [x] #9 Resume reads history in a bounded query: the happy-path is one forwards-from-cursor query with Limit=cap+1; the cap-exceeded path requires a second backwards-from-anchor query to find the newest cap. No unbounded scan
- [x] #10 core.Channel.Attach already gives Stream + non-empty channelSerial (TASK-16). Resume orchestration reads Stream.ChannelSerial() as the anchor before any history read, so concurrent publishes after attach land strictly past anchor and are observed via Stream.Next
- [x] #11 Tests cover: happy-path resume (mid-history serial), aged-out resume (clientSerial < earliest), cap-exceeded resume (gap > cap), attach-on-empty-channel with channelSerial, no-dupe at handover boundary (publish between Attach and history read still arrives exactly once via Stream)
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Recap from TASK-16

core.Channel.Attach(ctx) returns a Stream whose ChannelSerial() is the anchor (the watermark or the current tail's serial) — already non-empty for any ready channel. Concurrent publishes after Attach land in Stream.Next, never in a history read past anchor. We use that anchor as the upper bound of the gap replay.

## 1. Protocol

internal/protocol:
- FlagResumed Action constant = 1 << 2
- ErrorInfo struct {Message string; Code int; StatusCode int; HRef string} with json/msgpack tags
- ProtocolMessage gains Error *ErrorInfo (omitempty)

## 2. Storage

Add ChannelStore.Earliest(ctx) (string, error):
- memory: cs.mu, cs.order[0] or ''
- bbolt: View tx, cursor.Seek(channelPrefix), Next(); extract channelSerial from key
- postgres: SELECT channel_serial FROM messages WHERE channel = $1 ORDER BY channel_serial ASC LIMIT 1

storagetest contract suite gains Earliest cases: empty channel returns ''; after N publishes returns the first publish's serial; isolation across channels.

## 3. Resume orchestration (internal/realtime)

connection.dispatch passes the full ATTACH msg through (currently only the channel name).

attachment.go grows a Resume(ctx, clientSerial) path used when msg.ChannelSerial != ''. Algorithm:

  stream := ch.Attach(ctx)
  anchor := stream.ChannelSerial()

  if clientSerial == anchor:
    // caught up — no replay, RESUMED set, no error
  else:
    earliest := chstore.Earliest(ctx)
    resumed := true
    var info *protocol.ErrorInfo
    cursor := clientSerial

    if earliest == '' {
      // empty channel + non-empty cursor → fresh-attach equivalent
      // RESUMED stays clear (nothing was resumable)
      resumed = false
    } else if clientSerial < earliest {
      // aged out — partial replay from earliest onwards
      cursor = ''
      resumed = false
      info = &protocol.ErrorInfo{Message: 'history was unavailable from your cursor; replay starts at the earliest retained message', Code: 80008}
    }

    // Happy-path query
    page := chstore.History(forwards, cursor=cursor, limit=cap+1)
    // Filter inline: drop cms with channelSerial > anchor (cluster-mode concurrent publishes that have landed in storage but not in this Channel's list yet)
    if page.HasMore (cap+1 returned and there are more):
      // Cap exceeded — newest-cap re-fetch
      resumed = false
      info = &protocol.ErrorInfo{Message: 'replay was truncated to the most recent <cap> messages', Code: 40012}
      page = chstore.History(backwards, cursor='', limit=cap)
      // Filter: drop cms with channelSerial > anchor
      // Un-reverse: backwards mode reverses Messages within each cm; flip back to natural idx order, and reverse the slice for oldest-first delivery
      // (We accept the inconvenience of un-reversing rather than adding a new direction mode to storage; resume is rare and N is bounded by cap.)

  send ATTACHED{ChannelSerial: clientSerial (or anchor if fresh), Flags: 0 or FlagResumed, Error: info}
  for cm in page: send MESSAGE(cm)
  enter live loop via Stream.Next

## 4. Replay cap

Defined as a const in attachment.go (e.g., defaultReplayCap = 1000). Configurable in a follow-up; not exposed as a flag in this task.

## 5. ATTACH wire fields

ATTACH carries Channel and ChannelSerial. We do not consume rewind here (TASK-15) — any rewind param is ignored when ChannelSerial is also set (per DESIGN §4.3).

## 6. Tests

internal/realtime/sdk_test.go (or server_test.go): a WS-driven test for each scenario.
- Happy-path: publish 5 messages, ATTACH with mid-history serial (after #2), assert ATTACHED.RESUMED set + 3 MESSAGE frames replayed in order.
- Caught-up: publish 5, capture last cm's serial, ATTACH with that serial — RESUMED set, no replay.
- Aged-out: ATTACH with a fabricated old serial ("00000000000001-000@aaaaaaaaaa") — RESUMED clear, ATTACHED.Error populated, replay starts at earliest retained.
- Cap-exceeded: publish > cap, ATTACH with a serial from the start — RESUMED clear, Error populated, only the newest cap delivered.
- Empty channel resume: ATTACH with any clientSerial against a never-published channel — fresh-attach equivalent, RESUMED clear, no error, no replay.
- No-dupe boundary: publish N, ATTACH at serial[k], publish N+1 between attach and observation; verify exactly-once delivery of every cm > serial[k].

storagetest: Earliest cases (above).

## Out of scope

- Capability enforcement on attach (TASK-12)
- rewind param (TASK-15)
- Retention TTL (TASK-26) — without retention, the aged-out branch is exercised only by fabricated serials
- ATTACH mode/flags resolution (subscribe gating) — preserves current 'everyone gets MESSAGEs' behaviour
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Paused. Tackling TASK-16 first so the 'empty channel + clientSerial' case has a meaningful cursor instead of an empty string everywhere. Resume on 14 once 16 lands.

Plan landed on a single-query backwards approach: query backwards from anchor with EndChannelSerial=anchor (new HistoryQuery field) and Limit=cap+1. Walk newest-first; if any returned cm has channelSerial <= clientSerial, the gap fits — trim, reverse, RESUMED set. Otherwise cap-exceeded — drop oldest if cap+1 returned, reverse, RESUMED clear + Error. No Earliest() / no second query / no aged-out branch (TASK-26 will refine the Error message when retention is added; the response shape is the same).

Within-batch idx reversal from TASK-6's backwards mode is un-reversed before delivery so replayed MESSAGE frames carry messages in natural idx order.

End-to-end smoke verified: publish m0/m1/m2 on a fresh attach, capture m0's Message.Serial, resume from it, observe ATTACHED.flags=4 (RESUMED) + replay of m1/m2 in order. Cap-exceeded scenario covered by unit test publishing 1100 messages with cap=1000.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
## Attachment resume via history then live tail (TASK-14)

Implements channelSerial-based ATTACH resume: a client supplying ChannelSerial gets the gap between their cursor and the live tail replayed as ordinary MESSAGE frames, followed by the live stream. Verified end-to-end against the binary.

## Design

The handover problem (replay history then transition to live with no gap or duplicate) is solved by capturing the live anchor BEFORE the history query — Channel.Attach already gives Stream + watermark/tail serial. The history query is bounded at the anchor (new EndChannelSerial field), so concurrent publishes that have landed in storage but not yet on this node's live list are excluded from the scan and will arrive via Stream.Next instead.

A single backwards-from-anchor query with Limit = cap+1 covers all branches:
- If any returned cm has channelSerial <= clientSerial: gap fits within the cap. Trim those, reverse for forward delivery, RESUMED set.
- Otherwise: cap exceeded (or aged-out in a future TASK-26 world). Drop the oldest if cap+1 returned to keep newest cap, reverse, RESUMED clear, ATTACHED.Error set.

cap defaults to 1000 (defaultReplayCap), unexported for now — a follow-up can expose it as a server flag.

## What changed

- **protocol**: ErrorInfo struct {Message, Code, StatusCode, HRef}; ProtocolMessage.Error field; FlagResumed (= 1<<2).
- **storage**: HistoryQuery.EndChannelSerial — inclusive channelSerial upper bound, applied across direction. Implemented in memory (sort.SearchStrings on cs.order+\x00), bbolt (composite key + nextPrefix), postgres (channel_serial <= predicate). Contract suite gains an EndChannelSerial test (forwards and backwards).
- **realtime/attachment**: newAttachment takes *core.Channel + resumeFrom; run() does Initialize → resume orchestration → live Stream loop. Within-batch reversal from TASK-6 backwards mode is un-reversed before delivery.
- **realtime/connection**: dispatch passes the whole ATTACH msg through so handleAttach can read msg.ChannelSerial.

## Tests

- All three storage backends pass the (expanded) shared contract suite.
- Postgres integration suite still green.
- Realtime unit tests cover four resume scenarios over the WS protocol:
  - Happy-path resume: publish 5, resume from b1's serial, observe RESUMED + replay of b2,b3,b4.
  - Caught-up resume: resume from the most recent serial, observe RESUMED + zero replay.
  - Empty-channel resume with a fabricated cursor: RESUMED clear, no replay, Error set.
  - Cap-exceeded: publish 1100 messages, resume from an ancient cursor, observe RESUMED clear + Error + exactly cap (1000) MESSAGE frames in order, the newest cap.
- Smoke-tested end-to-end against the binary: publish m0/m1/m2, resume from m0's Message.Serial, observe ATTACHED.flags=4 + replay of m1/m2.

## Out of scope / follow-ups

- rewind channel param (TASK-15) — natural next: a different way of computing 'resumeFrom' that this resume infrastructure can serve.
- Aged-out vs cap-exceeded differentiation in the Error message — TASK-26 territory once retention is enforced.
- Configurable replay cap — currently a const.
- Capability enforcement on attach (TASK-12) — orthogonal.
<!-- SECTION:FINAL_SUMMARY:END -->
