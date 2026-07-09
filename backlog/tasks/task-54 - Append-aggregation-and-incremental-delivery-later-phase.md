---
id: TASK-54
title: Append aggregation and incremental delivery (streamed appends)
status: Done
assignee:
  - '@claude'
created_date: '2026-06-13 14:46'
updated_date: '2026-07-09 14:32'
labels:
  - mutable-messages
dependencies:
  - TASK-50
  - TASK-52
  - TASK-53
documentation:
  - DESIGN.md
priority: high
ordinal: 54000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Per DESIGN.md section 13.3. Implement append semantics on top of the core update/delete path. An append concatenates its data onto the message's current latest version (name and extras follow the same shallow-mixin replace as update). The server maintains the rolled-up latest data: a caught-up subscriber receives each append incrementally (just the delta data), while the first delivery for a message a subscriber has not yet seen — e.g. immediately after attach — is a full action=update with the aggregated payload, after which it receives subsequent appends incrementally. The server may conflate: coalesce multiple appends, drop superseded intermediate versions, or deliver an append as a full rolled-up update; the only guarantee is that the last version a subscriber receives is the most recent. Appends are not retained as individual entries in version history (only the aggregated latest is durable). A channel param lets a subscriber opt into full versions instead of incremental appends. This is explicitly a later phase, after the core update/delete path ships.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 append concatenates data onto the latest version; name and extras follow shallow-mixin replace
- [x] #2 A caught-up subscriber receives incremental appends; the first delivery for a not-yet-seen message is a full action=update with the aggregated payload
- [x] #3 The server may conflate appends and intermediate versions; the last delivered version is guaranteed to be the most recent
- [x] #4 Appends are not retained as individual entries in version history; the aggregated latest version is durable
- [x] #5 A channel param lets a subscriber opt into full versions instead of incremental appends
- [x] #6 Tests cover incremental delivery, full-on-attach aggregation, conflation, and the full-versions channel param
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. protocol: add Message.Alt map[string]*Message (json:"-", msgpack alt) + const DeltaAppend="delta-append"; helper HasAppendDelta. Add ParamAppendMode/AppendModeFull consts.
2. storage.MergeVersion -> return (*Message,error): append builds full-aggregate action=update Message with Data=concat, plus Alt[delta-append]=delta Message (action=append, delta data, same Version). concatData returns ErrIncompatibleAppend on string/binary type mismatch. update/delete unchanged.
3. Propagate MergeVersion error through memory/bbolt/postgres Mutate; map ErrIncompatibleAppend to 400/NACK in rest+realtime handlers.
4. AC#4 versions collapse (read-time): shared CollapseAppendVersions collapses runs of append-aggregates to their last. memory/bbolt call it before PaginateVersions (detect via Alt). postgres: is_append column (migration 0007) + LEAD window SQL collapse, pagination stays in SQL. Log keeps every append cm (append-only preserved) for live/resume fan-out; only versions read-path collapses.
5. attachment: parse appendMode param; per-attachment seen-set of message serials; forward(cm,backlog): appendAsDeltas = !backlog && appendMode!=full && all appends' serials already seen; deltas -> swap message to Alt delta (action=append); else deliver full aggregate action=update with Alt stripped. First-sight/backlog/appendMode=full => full update.
6. Tests: storagetest contract (aggregate, versions-collapse, incompatible-append) across memory+bbolt+postgres; realtime WS tests (incremental delivery, full-on-attach, conflation/last-version guarantee, appendMode=full).
7. Update DESIGN §13.3 (appendMode=full param, aggregate+delta mechanism) and §13.4 (appends collapse in versions index, not enumerated).
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Channel param name (from ../realtime go/realtime/lib/channel/params.go): appendMode=full (const AppendModeFull). Same param the reference uses to opt a subscriber out of incremental append deltas into full rolled-up versions. Also adopted the reference's transit representation: an append is stored/fanned-out as a full action=update carrying the aggregate, with the incremental delta in alt["delta-append"] (const protocol.DeltaAppend), an action=append message sharing the version. The delivery path (attachment) resolves alt to a delta for caught-up subscribers or strips it for full delivery; alt never reaches a client (json:"-").

AC#4 reconciliation with the append-only log (§6): the log keeps every append cm (needed for live fan-out + resume replay), so it stays append-only. Only the versions READ-path collapses: a maximal run of append aggregates reduces to its last (most-aggregated) version, while create/update/delete are verbatim. memory/bbolt collapse in Go via storage.CollapseAppendVersions before pagination (detecting appends via alt); postgres collapses in SQL with a LEAD(is_append) window (new is_append column, migration 0007) so pagination stays in SQL. GET .../messages/{serial}/versions therefore shows a streamed append as one evolving version, not one entry per delta. DESIGN §13.3/§13.4 updated to match.

Conflation approach: minimal/simplest-correct. We do not drop live cms, so ordered+complete delivery makes the last-version guarantee hold trivially; the one conflation we implement is full-on-first-sight — a fresh/late subscriber collapses the entire unseen pre-attach append run into a single full update (the intermediates it never saw are never sent). Backlog replay (resume/rewind) also forces full updates. Capability: append flows through message-update-* via mutationOps() (append is not delete) — unchanged and covered.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Implemented streamed-append aggregation and incremental delivery per DESIGN §13.3. An append is stored/fanned-out as a full action=update carrying the rolled-up aggregate plus the incremental delta in alt[delta-append] (storage.MergeVersion). concatData now rejects incompatible concatenation (string/binary only) with ErrIncompatibleAppend, mapped to 400 (REST) / NACK (WS). The attachment tracks per-subscriber seen serials and resolves each append to a delta (action=append) for caught-up subscribers or a full action=update on first sight / backlog / appendMode=full. The append-only log retains every append cm for live+resume fan-out; the versions read-path collapses append runs to the aggregate (shared CollapseAppendVersions for memory/bbolt; LEAD(is_append) SQL + migration 0007 for postgres). Added storagetest contract coverage (aggregate, delta carrier, versions-collapse incl. mixed update/append runs, incompatible-append) exercised across memory+bbolt+postgres; realtime WS tests (incremental delivery, full-on-first-sight, last-version guarantee, appendMode=full, incompatible NACK); REST tests (aggregate read, no-alt-leak, versions collapse, 400). DESIGN §13.3/§13.4 updated. All of go build/vet/vet -tags=integration/test pass, plus -race on realtime and the postgres -tags=integration -race suite (Docker).
<!-- SECTION:FINAL_SUMMARY:END -->
