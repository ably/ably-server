---
id: TASK-6
title: 'Add REST history endpoint (GET /channels/{name}/messages)'
status: Done
assignee:
  - '@lmars'
created_date: '2026-05-31 16:05'
updated_date: '2026-06-01 13:59'
labels: []
dependencies: []
ordinal: 6000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Implement GET /channels/{channel}/messages (DESIGN.md §2.2) returning paginated channel history, backed by storage.ChannelStore.History (already defined in internal/storage/storage.go). Requires the `history` capability (§3.1). Paginate via Ably's Link header convention (first, next) per §2.2, and support the usual direction/limit query params consistent with Ably. Implement in internal/rest and register on the mux in cmd/ably-server/main.go alongside the existing POST publish handler. Remove the "History is not yet implemented" note from the package doc once done.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 storage.HistoryQuery supports Direction (Forwards|Backwards), Start/End ms-since-epoch bounds (inclusive, 0=unbounded), opaque Cursor, and Limit; AfterChannelSerial field removed and callers updated
- [x] #2 All three backends (memory, bbolt, postgres) implement the expanded HistoryQuery, including backwards iteration and time-bound filtering via channelSerial range predicates (no Postgres schema change)
- [x] #3 Storage contract suite (storagetest) covers backwards/forwards, start/end bounds, cursor advancement in each direction, empty channel, and start>end empty case
- [x] #4 GET /channels/{name}/messages handler implemented in internal/rest, returning a flat []Message array (default JSON, application/x-msgpack supported via Accept)
- [x] #5 Query params match Ably SDK: direction (default backwards), start, end (ms), limit (default 100, capped 1000); invalid values return 400
- [x] #6 Endpoint requires Basic API key auth (returns 401 without); capability enforcement deferred to TASK-12
- [x] #7 Link header advertises rel="current", rel="first", and (when HasMore) rel="next" with an opaque cursor encoding the boundary channelSerial
- [x] #8 core.Channel exposes History(ctx, q) wrapping the per-channel ChannelStore.History
- [x] #9 Endpoint registered on cmd/ably-server/main.go mux as GET /channels/{name}/messages
- [x] #10 'History is not yet implemented' note removed from internal/rest package doc; history listed alongside other endpoints
- [x] #11 REST unit tests cover: auth required, empty channel, default backwards order, forwards order, start/end bounds, cursor traversal across multiple pages, 400 on invalid params, msgpack round-trip
- [x] #12 When Direction=Backwards, returned messages are fully reversed including the idx order within each atomic publish; Direction=Forwards preserves natural (serial,idx) order. Verified against Ably's API: backwards returned [b1,b0,a2,a1,a0] for batches [a0,a1,a2] then [b0,b1]
- [x] #13 storage.HistoryQuery.Limit counts Messages (not ChannelMessages) and Cursor is a Message.Serial; cursor lands mid-batch correctly and Limit can split an atomic publish. Verified against Ably's REST API empirically.
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Scope (expanded after Ably REST spec comparison + empirical verification)

Implement full SDK-compatible history: direction (default backwards), start/end ms bounds, limit, opaque cursor pagination, flat []Message response.

## Empirically verified behaviour (against local Ably API)

With Direction=Backwards, results are FULLY reversed — both across atomic publishes AND within each publish batch. Forwards preserves natural (serial, idx) order. The storage layer is responsible for emitting ChannelMessages in the requested direction with Messages within each ChannelMessage reversed when backwards. Storage must COPY when reversing — never mutate persisted state.

## 1. Storage interface (internal/storage/storage.go)

  type Direction uint8
  const (
      DirectionBackwards Direction = iota // zero value = Ably default
      DirectionForwards
  )

  type HistoryQuery struct {
      Direction Direction
      Start     int64  // inclusive ms-since-epoch; 0 = unbounded
      End       int64  // inclusive ms-since-epoch; 0 = unbounded
      Cursor    string // opaque channelSerial: forwards = strictly > cursor, backwards = strictly < cursor. Empty = no cursor
      Limit     int    // 0 = no limit
  }

Doc: 'For Direction=Backwards, ChannelMessages are returned newest-first AND each ChannelMessage's Messages slice is returned in reverse-idx order. The Messages slice is a fresh slice — persisted state is never mutated.'

Drop AfterChannelSerial (folds into Cursor under DirectionForwards). Audit all callers.

## 2. Backend implementations

a) memory: iterate cs.order forwards or backwards; apply timestamp bounds via lex compare against fmt.Sprintf("%014d-", t); apply Cursor; honour Limit + HasMore. For backwards, copy each ChannelMessage and reverse its Messages slice.

b) bbolt: bolt cursor with Seek+Next (forwards) or Seek+Prev (backwards). Apply lex bounds inline. For backwards, copy + reverse Messages slice.

c) postgres: SQL where channel = $1 AND channel_serial range predicates (time bounds + cursor predicate) ORDER BY channel_serial ASC|DESC LIMIT $N+1. When DESC, also ORDER BY idx DESC within ChannelMessages, OR more simply: group rows then reverse the Messages slice per ChannelMessage in Go before returning. No schema change.

## 3. Storage contract tests (internal/storage/storagetest)

- Backwards: returned ChannelMessages newest-first; Messages within each batch in reverse idx order
- Forwards: natural (serial, idx) order
- start/end bounds (inclusive)
- Cursor advancement in both directions
- Empty channel, start>end → empty page, HasMore=false
- Storage does not mutate persisted state (publish, then run a backwards history; subsequent forwards history must still see Messages in idx order)

## 4. REST handler (internal/rest/server.go)

- Authenticate (cap check = TASK-12)
- Parse direction (backwards default | forwards), start, end, limit (default 100, max 1000), opaque 'from' cursor; 400 on invalid
- Accept header → application/json (default) | application/x-msgpack
- Call manager.GetChannel(name).History(ctx, q)
- Flatten []ChannelMessage → []Message preserving storage order (storage already returns in the desired direction)
- Link header: rel=current (request URL), rel=first (request URL minus from), rel=next (request URL with from=lastCM.ChannelSerial, only when HasMore)
- 401 unauth'd, 400 invalid, 200 [] on empty/unknown channel

## 5. core layer

Channel.History(ctx, q) wraps ChannelStore.History (mirrors Channel.Publish).

## 6. Wire route (cmd/ably-server/main.go)

mux.HandleFunc("GET /channels/{name}/messages", rs.HandleHistory)

## 7. Cleanup

Remove 'History is not yet implemented' from internal/rest package doc.

## 8. Tests

REST: 401, empty channel, default backwards order returns fully-reversed sequence, forwards returns natural order, start/end bounds filter, multi-page cursor traversal in both directions, 400 on invalid params, msgpack round-trip.

## Out of scope

- text/html responses
- Capability enforcement (TASK-12)
- Retention bounds (TASK-26)
- Stamping Message.ID server-side (TASK-18) and richer publish response body — separate concerns observed during empirical check
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Verified Ably's history ordering empirically against a local Ably instance. Backwards direction reverses fully (across publishes AND within atomic publish batches). Forwards preserves natural order. Updated plan + AC accordingly; storage backends must reverse Messages within ChannelMessage when Direction=Backwards (with a copy — must not mutate persisted state).

After implementing the initial design, an end-to-end smoke test revealed that 'limit' counts Messages in Ably (not ChannelMessages) and splits multi-message atomic publishes across pages, with the 'next' link cursor being a full Message.Serial. Re-verified against the live Ably API and refactored:
- storage.HistoryQuery.Limit now counts Messages; Cursor is a Message.Serial (channelSerial:idx) compared lex-strictly in the requested direction; pages may begin/end with partial ChannelMessages.
- All three backends (memory, bbolt, postgres) iterate at Message granularity. Postgres uses a (channel_serial, idx) lex compare with LIMIT N+1 and trims partial batches in Go.
- REST emits 'next' cursors as the trailing Message.Serial of the page.
- Storage contract suite gained mid-batch forwards/backwards cursor tests and a 'limit splits atomic batch' test.
- REST tests cover the 5-message-batch + limit=2 scenario.
- Smoke-tested binary end-to-end: batch [m0..m4], backwards limit=2 returns [m4,m3] with rel=next pointing at m3's Message.Serial; following the cursor returns [m2,m1]. Matches Ably's wire behaviour.

Added serial.ParseMessageSerial and serial.TimestampBounds for cross-backend reuse.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
## REST history endpoint (TASK-6)

Implements GET /channels/{name}/messages, returning Ably-SDK-compatible paginated message history. Hooks up to the existing storage backends via a substantially extended HistoryQuery.

## What changed

- **storage.HistoryQuery**: dropped AfterChannelSerial; added Direction (Forwards/Backwards, default Backwards to match Ably), Start/End ms-since-epoch bounds (inclusive, 0=unbounded), and an opaque Cursor that is a Message.Serial ('<channelSerial>:<idx>') compared lex-strictly in the requested direction. Limit now counts Messages, not atomic publishes — so a multi-message batch can be split across pages with the trailing/leading ChannelMessage carrying a subset of its Messages. Backends emit ChannelMessages without ever mutating persisted state.
- **memory / bbolt / postgres**: each iterates at Message granularity. Time bounds piggy-back on the timestamp prefix embedded in channelSerial (DESIGN.md §8) via the new serial.TimestampBounds helper — no schema change in Postgres. Cursor is decomposed into (channelSerial, idx) via the new serial.ParseMessageSerial helper.
- **storage contract suite**: extended with backwards/forwards ordering (including the verified full-reversal-within-batch), start/end filtering, mid-batch cursor pagination in both directions, limit-splits-atomic-batch, no-mutation-of-persisted-state, and the new empty-channel and end<start cases.
- **internal/rest**: HandleHistory parses direction/start/end/limit + the internal opaque 'from' cursor, supports JSON (default) and msgpack via Accept, returns a flat []Message, and emits 'Link' headers with rel=current, rel=first, and (when HasMore) rel=next carrying the trailing Message.Serial.
- **core.Channel.History**: thin wrapper over ChannelStore.History, mirroring Channel.Publish.
- **cmd/ably-server**: route registered.

## Verified

- Behaviour compared empirically with a live Ably system (port 8081). Discovered and corrected two assumption-driven mistakes during the work: (1) backwards reverses everything including within-batch idx order; (2) limit slices Messages and the next cursor is a Message.Serial, not a channelSerial.
- All three backends pass the (now larger) shared contract suite. Postgres testcontainer suite passes. REST handler unit tests cover auth, empty channel, default backwards, forwards, mid-batch split, cursor traversal, invalid-param rejection, msgpack round-trip, and Link header invariants.
- End-to-end smoke against the freshly-built binary: 5-message single batch → backwards limit=2 returns [m4,m3] with rel=next carrying '<channelSerial>:003'; following the cursor returns [m2,m1].

## Out of scope / follow-ups

- Capability enforcement on the 'history' op (TASK-12).
- Match Ably's publish response body shape and the Message.action field — empirically observed during this work and filed as TASK-36 and TASK-37 respectively.
- TASK-29 (LISTEN reconcile) references the removed AfterChannelSerial field in its description; the implementer will adapt to Cursor when that lands.
<!-- SECTION:FINAL_SUMMARY:END -->
