---
id: TASK-51
title: 'Capabilities: message-update/delete-own/any with ownership resolution'
status: To Do
assignee: []
created_date: '2026-06-13 14:46'
labels:
  - auth
dependencies:
  - TASK-50
  - TASK-12
documentation:
  - DESIGN.md
ordinal: 51000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Per DESIGN.md sections 3.1 and 13.5. Add the four capability ops message-update-own, message-update-any, message-delete-own, message-delete-any (append is gated by message-update-*). Extend capability resolution with an ownership dimension, the first ownership-scoped op in the model: -own resolves only if the caller's resolved clientId (section 3.2) equals the target message's creator clientId; -any waives the check. The creator clientId comes from the message's current version in storage (TASK-50). Wire the check into both the realtime and REST mutation paths. Creating a message stays plain publish; all version reads stay history.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The four message-{update,delete}-{own,any} ops parse from x-ably-capability and from the API key capability
- [ ] #2 An update/append authorises against message-update-own/any; a delete against message-delete-own/any
- [ ] #3 -own permits the op only when the caller's resolved clientId equals the target message's creator clientId; -any skips the check
- [ ] #4 Insufficient capability is rejected (WS NACK or ERROR; REST 401)
- [ ] #5 Creating a message remains plain publish; version reads remain history
- [ ] #6 Tests cover own-allowed, own-denied (different clientId), any-allowed, and missing-capability
<!-- AC:END -->
