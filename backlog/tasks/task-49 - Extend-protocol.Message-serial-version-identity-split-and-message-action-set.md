---
id: TASK-49
title: 'Extend protocol.Message: serial/version identity split and message action set'
status: Done
assignee: []
created_date: '2026-06-13 14:45'
updated_date: '2026-06-14 19:59'
labels:
  - protocol
dependencies:
  - TASK-37
documentation:
  - DESIGN.md
ordinal: 49000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Per DESIGN.md section 8 and section 13.1. Reframe Message.Serial as the stable message identity (unchanged across versions; for a create it equals the create's channelSerial:idx). Add a Version field naming a single version of a message — a MessageVersion carrying its own serial (channelSerial:idx of the publish that produced it), timestamp, the operating clientId, an optional description, and optional metadata. For a create, version equals serial; each subsequent update/delete/append gets a fresh, strictly-greater version. Extend the MessageAction enum to the values mutable messages needs (create 0, update 1, delete 2, append 5), pinned to ably-go's constants. Builds on TASK-37, which added the action field and rejected non-zero inbound; this task defines the values and the version object the rest of mutable messages needs. No mutation handling yet (that is later tasks).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Message.Serial is the stable message identity; for a create it equals channelSerial:idx and is documented as unchanged across versions
- [x] #2 Message gains a Version field (MessageVersion: serial, timestamp, clientId, description, metadata) with json+msgpack tags matching Ably's version object
- [x] #3 MessageAction constants defined: create=0, update=1, delete=2, append=5, pinned to ably-go's MessageAction
- [x] #4 A create stamps version == serial; the types represent a mutation as serial = target identity with version = the new position
- [x] #5 A Message carrying action and version round-trips through JSON and msgpack
- [x] #6 Outbound creates still carry action=0 (TASK-37 behaviour preserved); inbound non-zero actions remain rejected until the mutation path lands
<!-- AC:END -->
