---
id: TASK-14
title: Support resuming an attachment via history then live tail
status: To Do
assignee:
  - '@lmars'
created_date: '2026-05-31 16:11'
updated_date: '2026-06-01 14:45'
labels: []
dependencies: []
ordinal: 14000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Implement channelSerial-based attachment resume per DESIGN.md §4.3/§4.4. On ATTACH carrying a channelSerial, the attachment first reads the gap from storage history (storage.History with AfterChannelSerial = the client's cursor) up to the channel's current head, forwarding those ChannelMessages, then transitions to the live linked-list tail at the resume point — with no lost or duplicated messages across the handover. If the supplied serial has aged out of retention, attach at the live head instead, clear ATTACHED.flags.RESUMED, and populate ATTACHED.error with an ErrorInfo per §4.3 (no replay). ATTACHED.channelSerial reflects the confirmed attach point. This establishes the history-then-live cursor mechanism that rewind reuses.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 On ATTACH carrying a non-empty ChannelSerial, the server replays history strictly after that serial up to the live attach point, then continues from the live linked list — with no gap and no duplicate cms across the handover
- [ ] #2 ATTACHED.channelSerial reflects the confirmed attach point: the client's supplied serial in the resume case, the current live-head serial in the fresh-attach and aged-out cases
- [ ] #3 ATTACHED.flags.RESUMED is set when replay succeeded (history-then-live happens), cleared otherwise
- [ ] #4 On aged-out resume (client's serial sorts before the channel's earliest retained serial), the attachment is created at the live head with no replay; ATTACHED carries an ErrorInfo explaining the discontinuity
- [ ] #5 protocol.ProtocolMessage gains an Error *ErrorInfo field and protocol.FlagResumed is defined
- [ ] #6 ATTACH for a channelSerial against an empty/unknown channel attaches at the live head with RESUMED clear (no replay needed, no error)
- [ ] #7 Replayed messages stream as ordinary MESSAGE frames in publish order, one frame per ChannelMessage with the full batch in Messages[]
- [ ] #8 Resume reads history in pages (Limit-bounded, cursor-advancing) so a long replay does not allocate the entire backlog at once
- [ ] #9 Tests cover: happy-path resume (publishes, ATTACH with mid-history serial, observe replay then live), aged-out resume, attach-on-empty-channel with channelSerial, multi-page history replay, no-dupe at handover boundary
- [ ] #10 core.Channel exposes the operation atomically: a single call captures both the resume-anchor serial AND the Stream cursor positioned at that serial, so concurrent publishes between history-read and live-attach cannot tear
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Approach

The handover problem: replay history then transition to the live linked list with no gap and no duplicate. We solve it by anchoring on the live cursor *first*, capturing its current channelSerial, and then reading history up to that anchor before pulling from the cursor.

## 1. Protocol surface

- internal/protocol: add FlagResumed (= 1 << 2, per Ably) and ErrorInfo {Message, Code, StatusCode, HRef} with json/msgpack tags
- protocol.ProtocolMessage gains Error *ErrorInfo with omitempty

## 2. core layer

Add Channel.AttachAt(clientSerial string) (stream *Stream, anchor string, err error). It:
- locks the channel
- captures cur := c.tail
- returns Stream{cursor: cur} AND the anchor = cur.cm.ChannelSerial (or '' if at sentinel/empty channel)

Stream.Next() unchanged — it still yields strictly-later cms past the captured anchor.

The atomicity guarantee: under c.mu we capture both stream cursor and anchor in one critical section. New publishes after that linkage land in Stream.Next, never in the history read.

## 3. Storage layer

Add ChannelStore.Earliest(ctx) (string, error). Returns the smallest retained channelSerial (or '' if the channel is empty). All three backends:
- memory: cs.order[0] (or '') under cs.mu
- bbolt: View tx, Seek(channelPrefix), Next; decode key (skip prefix), return channelSerial portion
- postgres: SELECT channel_serial FROM messages WHERE channel = $1 ORDER BY channel_serial ASC LIMIT 1

This is the aged-out detector: clientSerial < earliest means we can't prove gapless replay.

## 4. Resume orchestration

In internal/realtime/attachment.go, expand the constructor to accept the client's channelSerial. The run loop:

  ATTACH carries clientSerial:
    stream, anchor := channel.AttachAt(clientSerial)

    if clientSerial == '': fresh attach, no replay (skip to live loop)
    else if anchor == '': empty channel, no replay (skip to live loop, RESUMED set since there is nothing to lose)
    else:
      earliest := chstore.Earliest(ctx)
      if earliest != '' && clientSerial < earliest:
        // aged out: skip replay, ATTACHED.error, RESUMED clear, ATTACHED.channelSerial = anchor
        send ATTACHED{ChannelSerial: anchor, Error: …, Flags: 0}
        enter live loop
      else:
        // happy path: page through history (clientSerial, anchor], forward as MESSAGE frames
        send ATTACHED{ChannelSerial: clientSerial, Flags: FlagResumed}
        cursor := clientSerial
        for {
          page := History(forwards, cursor=cursor, limit=HistoryPageSize)
          for cm in page.ChannelMessages: send MESSAGE(cm)
          if !page.HasMore || cursor.channelSerial == anchor: break
          cursor = lastMessageSerial(page)
        }
        enter live loop (stream.Next from anchor)

Bound history reads to channel_serial <= anchor by passing Cursor + Direction + an upper-bound predicate. The cleanest approach: pass q.End = parseTimestamp(anchor) since channel_serial encodes timestamp — but two cms in the same ms could tie. Safer: iterate and break when current page's last channelSerial >= anchor.

Actually simplest correct approach: keep history queries Limit-bounded and watch for any returned cm whose ChannelSerial > anchor — discard those, mark done. In practice, with the AttachAt mutex captured, history strictly <= anchor is what will exist *until further publishes commit*; new publishes after AttachAt go straight onto the linked list and will be picked up by Stream.Next. So no double-emit risk.

Concretely: after each history page, if the last cm.ChannelSerial >= anchor, trim to <= anchor and stop. Otherwise continue with cursor = lastMessageSerial.

## 5. Dispatch wiring

connection.handleAttach changes signature to accept (ctx, msg). It extracts msg.Channel, msg.ChannelSerial, msg.Flags; calls newAttachment with the resume cursor and the requested flags.

## 6. Tests

internal/realtime/sdk_test.go:
- happy-path: publish a sequence, ATTACH at mid-history serial, expect ATTACHED w/ RESUMED + clientSerial, then replayed MESSAGE frames in order, then live publishes
- aged-out: ATTACH with a fabricated old serial (e.g. '00000000000001-000@zzzzzzzzzz'); expect ATTACHED w/ anchor serial, RESUMED clear, Error populated; no replay
- empty channel + clientSerial: ATTACH with any clientSerial against a never-published channel; expect ATTACHED w/ ChannelSerial='' (or anchor=''), RESUMED clear, no error, no replay
- multi-page: publish N>HistoryPageSize messages, ATTACH at start, verify all N replayed + ordering preserved
- handover no-dupe: publish 3, ATTACH at serial[0], replay should be [1,2]; immediately publish 4 (after ATTACH); attachment delivers 4 once

## 7. Out of scope

- rewind (TASK-15)
- Capability enforcement on subscribe (TASK-12)
- Retention enforcement (TASK-26); aged-out branch is wired but cannot trigger in production today
- ATTACH params other than channelSerial
- Mode/flag resolution (subscribe gate); we'll preserve current 'everyone gets MESSAGEs' behaviour
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Paused. Tackling TASK-16 first so the 'empty channel + clientSerial' case has a meaningful cursor instead of an empty string everywhere. Resume on 14 once 16 lands.
<!-- SECTION:NOTES:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 All three storage backends still pass the contract suite
- [ ] #2 go test -tags=integration ./... is green
- [ ] #3 Smoke-tested end-to-end against the binary
<!-- DOD:END -->
