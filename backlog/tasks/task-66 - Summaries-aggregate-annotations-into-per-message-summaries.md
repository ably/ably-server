---
id: TASK-66
title: 'Summaries: aggregate annotations into per-message summaries'
status: To Do
assignee: []
created_date: '2026-07-09 11:06'
updated_date: '2026-07-10 10:13'
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
- [ ] #1 An annotation create/delete updates the target message's summary and subscribers receive a MESSAGE_SUMMARY message carrying the new summary
- [ ] #2 Message history and single-message reads include the current summary
- [ ] #3 Works across nodes in cluster mode
- [ ] #4 All five v1 summarisation methods implemented (distinct.v1, unique.v1, multiple.v1, flag.v1, total.v1) with folds and wire shapes matching the reference (lib/ablyrpc annotation.go + message_test.go summary encodings)
- [ ] #5 Per DESIGN.md §14.2: the summary is folded transactionally at store time, stamped on the stored annotation cm, and delivered from that snapshot as a MESSAGE with action summary (4) to ordinary subscribe attachments — no separate rollup publish, no debounce
- [ ] #6 Unit tests mirror the reference's summary wire-shape tests; SDK-level verification is deferred to the ably-js run (TASK-68), since ably-go has no annotations API
<!-- AC:END -->
