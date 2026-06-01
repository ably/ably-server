---
id: TASK-37
title: Add action field to protocol.Message for SDK wire-compatibility
status: To Do
assignee: []
created_date: '2026-06-01 10:42'
labels: []
dependencies: []
priority: medium
ordinal: 37000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Ably's wire format includes an 'action' field on every Message (default 0 = publish/message-create). Observed empirically in the local Ably history response — every Message had "action": 0. SDKs parsing the wire format expect this field, even if only the publish-action variant is supported.

Empirically observed (history GET, local Ably):
  {"id":"...", "timestamp":..., "data":"...", "action": 0, "serial":"...", "name":"..."}

In the broader Ably protocol, MessageAction values include publish/create (0), update, delete, and a few others for object messages and message annotations. For an MVP open-source server we only need to emit action=0 on every message we produce; honouring inbound non-zero actions (update/delete) is a future feature.

Scope:
- Add Action (int or named MessageAction type) to internal/protocol.Message with JSON/msgpack tag 'action'
- Default value 0 (publish/message-create)
- Server stamps action=0 on every Message it emits (REST history, WS MESSAGE delivery)
- Inbound (publish path) currently accepts but ignores non-zero values — or rejects them with a clear error. Pick rejection initially so we don't silently drop semantics; tighten or relax once the relevant feature task lands

Out of scope:
- Implementing update/delete/annotation message actions (separate features in Ably's spec; not in our DESIGN.md scope yet)
- Defining the full MessageAction enum and persisting non-zero actions
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 protocol.Message has an Action field with json:"action" msgpack:"action" tags (no omitempty: action=0 must be emitted on the wire to match Ably)
- [ ] #2 Every outbound Message (REST history, WS MESSAGE delivery) carries action=0
- [ ] #3 Inbound publish requests with action!=0 are rejected with 400 (or appropriate WS error) until further actions are supported
- [ ] #4 Wire-format tests confirm action: 0 appears in JSON and msgpack history responses
<!-- AC:END -->
