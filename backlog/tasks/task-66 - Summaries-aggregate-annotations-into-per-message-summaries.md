---
id: TASK-66
title: 'Summaries: aggregate annotations into per-message summaries'
status: To Do
assignee: []
created_date: '2026-07-09 11:06'
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
- [ ] #2 Summary aggregation semantics match Ably for the supported summarisation methods (at minimum the methods ably-js exercises)
- [ ] #3 Message history and single-message reads include the current summary
- [ ] #4 Works across nodes in cluster mode
- [ ] #5 DESIGN.md documents the summary model
<!-- AC:END -->
