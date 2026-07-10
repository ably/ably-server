---
id: TASK-64
title: 'Annotations: storage and realtime publish/delivery'
status: To Do
assignee: []
created_date: '2026-07-09 11:05'
updated_date: '2026-07-10 10:13'
labels: []
dependencies:
  - TASK-63
priority: high
ordinal: 64000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
With the wire types in place (TASK-63), make annotations flow end to end over realtime. An inbound ANNOTATION frame publishes annotation.create / annotation.delete operations targeting an existing message serial; annotations land on the channel stream through the unified storage-to-Appender path (like presence, a distinct kind sharing the channelSerial namespace) and are delivered as outbound ANNOTATION frames only to attachments holding ANNOTATION_SUBSCRIBE. Publishing requires ANNOTATION_PUBLISH. The target message must exist within retention; the server stamps connectionId and (per §3.2 rules) clientId, and ACK/NACKs on msgSerial as for any publish. Reference realtime lib/channel/attachment.go (annotation handling around lines 1360-1910).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Inbound ANNOTATION frames with create/delete actions are validated (target serial exists, modes held) and persisted on the stream with server-assigned serials
- [ ] #2 Attachments with ANNOTATION_SUBSCRIBE receive outbound ANNOTATION frames; attachments without it do not
- [ ] #3 Publish without ANNOTATION_PUBLISH mode is NACKed
- [ ] #4 Works in memory, disk, and cluster modes (cluster via the normal NOTIFY path)
- [ ] #5 Behaviour matches DESIGN.md §14.1/§14.3: annotation cms are kind=annotation sharing the channelSerial namespace, message_serial holds the target serial, targets must exist in the latest-version projection, concrete clientId required except anonymous multiple.v1/total.v1 publishes, and the no-mode-bits ATTACH default excludes the annotation modes
<!-- AC:END -->
