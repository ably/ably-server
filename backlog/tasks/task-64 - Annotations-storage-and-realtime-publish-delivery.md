---
id: TASK-64
title: 'Annotations: storage and realtime publish/delivery'
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 11:05'
updated_date: '2026-07-10 10:34'
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
- [x] #1 Inbound ANNOTATION frames with create/delete actions are validated (target serial exists, modes held) and persisted on the stream with server-assigned serials
- [x] #2 Attachments with ANNOTATION_SUBSCRIBE receive outbound ANNOTATION frames; attachments without it do not
- [x] #3 Publish without ANNOTATION_PUBLISH mode is NACKed
- [x] #4 Works in memory, disk, and cluster modes (cluster via the normal NOTIFY path)
- [x] #5 Behaviour matches DESIGN.md §14.1/§14.3: annotation cms are kind=annotation sharing the channelSerial namespace, message_serial holds the target serial, targets must exist in the latest-version projection, concrete clientId required except anonymous multiple.v1/total.v1 publishes, and the no-mode-bits ATTACH default excludes the annotation modes
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Storage: add KindAnnotation; extend CMItems for annotations; add StoreAnnotation(ctx,anns) (validates each target in the latest-version projection -> ErrTargetNotFound, mints channelSerial, stamps Annotation.Serial=<cs>:<idx>, persists kind=annotation with message_serial=TARGET, idempotency via shared id index, fires appender; returns stored cm as the TASK-66 fold seam) and Annotations(ctx,messageSerial,q) (annotations-for-message scan via serial index, paginated by Annotation.Serial). Add PaginateAnnotations helper. Implement across memory (map[target][]*Annotation + log), bbolt (annotations bucket <ch>\0<target>\0<serial> + log), postgres (kind=annotation rows, message_serial=target served by channel_messages_serial_idx; NO migration -- kind+message_serial suffice). core.Channel: PublishAnnotation/Annotations wrappers; Append handles Annotations. Realtime: dispatch ActionAnnotation (msgSerial-gated); handleAnnotation gates ANNOTATION_PUBLISH mode+annotation-publish cap (NACK 40160), resolves/validates clientId+Annotation.Validate (anonymous multiple/total exception), stamps connectionId/clientId/timestamp, publishes via worker, ACK/NACK (target-not-found 40400). attachment.forward: annotation branch -> ANNOTATION frame only for ANNOTATION_SUBSCRIBE; message/presence history+resume skip annotation cms (kind). Extend storagetest with StoreAnnotation/Annotations contract cases. Refine DESIGN §14.3 to state raw ANNOTATION frames are live-only (like presence), gap-replay skips them.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
StoreAnnotation added to memory/bbolt/postgres: validates each target in the latest-version projection (ErrTargetNotFound, like mutations), mints channelSerial, stamps Annotation.Serial=<cs>:<idx>, persists kind=annotation with message_serial=TARGET, idempotent via the shared id index, fires the appender. No postgres migration needed — kind + message_serial (served by channel_messages_serial_idx) suffice; the pg cm decoder + History now handle the annotation kind so cluster NOTIFY delivery works. Annotations(target,q) serves the annotations-for-message scan (memory map, bbolt annotations bucket, pg serial-index query), paginated by Annotation.Serial. Realtime: handleAnnotation gates ANNOTATION_PUBLISH mode + annotation-publish cap (NACK 40160), resolves/validates clientId + Annotation.Validate (anonymous multiple/total exception), stamps connectionId/clientId/timestamp, publishes via the worker, ACK/NACK (target-not-found 40400); forward() emits ANNOTATION frames only to ANNOTATION_SUBSCRIBE, others skip. §14.3 refined: raw ANNOTATION frames are live-only; the message-stream resume gap skips them (like presence), while the summary (a message cm) is gap-replayed. TASK-66 seam: StoreAnnotation returns the stored cm and is the transactional point where the summary fold + snapshot-onto-cm will slot in at store time.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Annotations flow end-to-end over realtime and persist across all three backends. Inbound ANNOTATION frames are mode/capability-gated (NACK 40160), target-validated against the latest-version projection (NACK 40400), stamped and msgSerial-ACKed via the publish worker; delivery emits ANNOTATION frames only to ANNOTATION_SUBSCRIBE attachments (others skip, as with presence). StoreAnnotation persists kind=annotation cms sharing the channelSerial namespace with message_serial=target; the storagetest contract suite exercises StoreAnnotation/Annotations identically across memory/bbolt/postgres, and the postgres integration suite passes. Cluster delivery rides the existing NOTIFY path (cm decoder handles the annotation kind). No migration required. §14.3 refined for live-only raw annotation delivery.
<!-- SECTION:FINAL_SUMMARY:END -->
