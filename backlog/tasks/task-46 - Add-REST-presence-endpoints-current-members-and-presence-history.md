---
id: TASK-46
title: 'Add REST presence endpoints: current members and presence history'
status: To Do
assignee: []
created_date: '2026-06-13 09:38'
labels:
  - presence
dependencies:
  - TASK-42
  - TASK-43
documentation:
  - DESIGN.md
ordinal: 46000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Add the two read-only REST presence endpoints per DESIGN.md sections 2.2 and 12.6. GET /channels/{channel}/presence returns the current membership set from Members (members as PresenceMessages with action PRESENT) and requires the subscribe capability. GET /channels/{channel}/presence/history returns presence history via a kind=presence history scan, paginated with the same Link header convention as message history, and requires the history capability. Both accept json and msgpack per Accept and Content-Type. There is no REST write surface for presence: a member is inherently bound to a realtime connection.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 GET /channels/{channel}/presence returns the current set as PresenceMessages (action PRESENT); an empty set returns an empty array
- [ ] #2 GET /channels/{channel}/presence/history returns presence-only history (no message cms), paginated with first/next Link headers like message history
- [ ] #3 The presence endpoint requires the subscribe capability; presence history requires the history capability; insufficient capability returns 401
- [ ] #4 Both endpoints honour json and msgpack via Accept and Content-Type
- [ ] #5 No write or POST presence route exists
- [ ] #6 Tests cover current-set retrieval, presence-history pagination, capability rejection, and json+msgpack responses
<!-- AC:END -->
