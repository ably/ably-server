---
id: TASK-66
title: 'Summaries: aggregate annotations into per-message summaries'
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 11:06'
updated_date: '2026-07-10 11:02'
labels: []
dependencies:
  - TASK-64
priority: high
ordinal: 66000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PDR-090's experimental scope includes annotation summaries: the server rolls up a message's annotations (e.g. distinct.v1 / multiple.v1 counts per annotation type) into a summary attached to the message, delivered to subscribers as a Message with action MESSAGE_SUMMARY (4) carrying the unchanged message serial and the summary object, and reflected in the latest-version projection so history and GET .../messages/{serial} return the current summary. Follows the same derived-state pattern as the latest-version fold (§13.4). Reference realtime lib/ablyrpc (MessageAction_MESSAGE_SUMMARY, latestIncorporatedAnnotation) and roles/qos/annotations.go.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 An annotation create/delete updates the target message's summary and subscribers receive a MESSAGE_SUMMARY message carrying the new summary
- [x] #2 Message history and single-message reads include the current summary
- [x] #3 Works across nodes in cluster mode
- [x] #4 All five v1 summarisation methods implemented (distinct.v1, unique.v1, multiple.v1, flag.v1, total.v1) with folds and wire shapes matching the reference (lib/ablyrpc annotation.go + message_test.go summary encodings)
- [x] #5 Per DESIGN.md §14.2: the summary is folded transactionally at store time, stamped on the stored annotation cm, and delivered from that snapshot as a MESSAGE with action summary (4) to ordinary subscribe attachments — no separate rollup publish, no debounce
- [x] #6 Unit tests mirror the reference's summary wire-shape tests; SDK-level verification is deferred to the ably-js run (TASK-68), since ably-go has no annotations API
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Fold design: add protocol.Summary (map[type]*Aggregation) with pure per-method folds (distinct/unique/multiple/flag/total) handling create+delete; key-aware JSON/msgpack codecs pinned to reference wire shapes. Message gains summary field (json+msgpack) riding the projection payload so reads carry it. Annotation gains an in-memory-only summary carrier (json:- msgpack:-) stamped by StoreAnnotation.

Backends: in StoreAnnotation, transactionally fold each annotation into the target message's projection Message.Summary (memory cs.latest, bbolt latest bucket, pg messages table) and stamp the post-fold snapshot onto the returned/in-memory annotation cm. For pg cluster determinism, persist the snapshot in a new channel_messages.summary column (migration 0008) and reconstruct it into Annotation.Summary on the LISTEN loadChannelMessage path, so a node that never saw earlier annotations emits the identical summary from the cm.

Delivery: attachment.forward, on an annotation cm, additionally sends ordinary SUBSCRIBE attachments a MESSAGE (ProtocolMessage action 15) whose Message has action=summary(4), the target's unchanged serial, and the snapshot summary; ANNOTATION_SUBSCRIBE still gets the raw ANNOTATION frame (both fire for dual-mode). One summary per annotation, no debounce.

Tests: table-driven wire-shape tests transcribed from reference TestMessageSummary_JSONEncoding + fold semantics from qos/annotations.go; storagetest contract extended (fold across all three backends, summary on reads); realtime delivery test; pg cross-node integration summary test.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implemented. Fold: protocol.Summary (map[type]*Aggregation) with pure per-method folds over (summary, annotation); Apply is copy-on-write so each annotation's stamped snapshot is independent within a batch. Wire shapes pinned by TestSummaryJSONEncoding (transcribed from ablyrpc TestMessageSummary_JSONEncoding); fold semantics (incl. delete per method) pinned by TestSummaryFold (transcribed from qos/annotations.go expectedSummary). Message gained a summary field (json+msgpack) on the projection; Annotation gained a server-internal summary carrier (json:- msgpack:-) stamped by StoreAnnotation.

Backends fold transactionally: memory (cs.latest copy), bbolt (latest bucket, in write tx), pg (messages projection UPDATE, in tx). pg cluster determinism: post-fold snapshot persisted in new channel_messages.summary column (migration 0008), reconstructed into Annotation.Summary on the LISTEN loadChannelMessage path — verified by TestPostgresClusterSummarySnapshotIsCrossNodeDeterministic (node2 never computes the fold, emits identical snapshot + projection read).

Delivery: attachment.forward emits one MESSAGE (action summary=4, target serial, snapshot) per annotation to SUBSCRIBE, plus the raw ANNOTATION to ANNOTATION_SUBSCRIBE; dual-mode gets both; raw frame does not leak the snapshot. No separate rollup cm, no debounce.

DESIGN refinement: §14.3 rewritten — the summary is derived from the annotation cm at live delivery (not a separate persisted cm), so it is not gap-replayed as a frame; a resuming subscriber reconverges via the projection-backed message reads (§14.4). action.go comment corrected (summary=4, meta=3).

Tests: protocol unit tests; storagetest contract extended (fold/snapshot/delete/reads across memory+bbolt+pg); realtime delivery tests (+race); pg integration cross-node + full suite (race). Full battery green: go build/vet/vet-integration/test, go test -race ./internal/realtime, go test -tags=integration -race ./internal/storage/postgres/...
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Aggregate annotations into per-message summaries (DESIGN.md §14.2/§14.3). Added protocol.Summary with five pure v1 folds (distinct/unique/multiple/flag/total) handling create+delete, wire shapes pinned to the reference. StoreAnnotation folds transactionally into the target's projection summary in all three backends and stamps a post-fold snapshot on the annotation cm; pg persists the snapshot in a new summary column so cluster nodes deliver deterministically off the cm. attachment.forward delivers a MESSAGE/summary (action 4) to SUBSCRIBE attachments and the raw ANNOTATION to ANNOTATION_SUBSCRIBE. Message reads carry the current summary via the projection.
<!-- SECTION:FINAL_SUMMARY:END -->
