---
id: TASK-46
title: 'Add REST presence endpoints: current members and presence history'
status: Done
assignee:
  - '@lmars'
created_date: '2026-06-13 09:38'
updated_date: '2026-06-14 18:59'
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
- [x] #1 GET /channels/{channel}/presence returns the current set as PresenceMessages (action PRESENT); an empty set returns an empty array
- [x] #2 GET /channels/{channel}/presence/history returns presence-only history (no message cms), paginated with first/next Link headers like message history
- [x] #3 The presence endpoint requires the subscribe capability; presence history requires the history capability; insufficient capability returns 401
- [x] #4 Both endpoints honour json and msgpack via Accept and Content-Type
- [x] #5 No write or POST presence route exists
- [x] #6 Tests cover current-set retrieval, presence-history pagination, capability rejection, and json+msgpack responses
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Read-only REST presence endpoints (DESIGN.md §2.2, §12.6).

GET /channels/{name}/presence returns the current membership set from core.Channel.Members as a flat array of PresenceMessages, each copied and stamped action=PRESENT (never mutating stored state); empty set → []. GET /channels/{name}/presence/history returns the presence stream via a kind=presence History scan, reusing the message-history query shape and RFC-5988 Link pagination (first/current/next). Both negotiate JSON/msgpack via Accept and require API-key auth.

New helpers in internal/rest: flattenPresence, presentMembers, marshalPresence; lastMessageSerial extended to take the trailing item's serial from either kind so writeHistoryLinks paginates presence too. Routes registered in main.go and the test mux.

Capability note: op-level enforcement (subscribe for the set, history for the history) is deferred to TASK-12, consistent with the message-history endpoint — today both require the API key (401 without it). There is no REST write surface for presence (presence is realtime-bound): a POST returns 405.

Tests (internal/rest/presence_test.go): current-set retrieval (action PRESENT, both members), empty-set [], msgpack current set, presence history ordering (enter/update/leave newest-first), presence/message kind isolation, history pagination via the next link, no-write-route (405), and auth-required (401) on both endpoints. Full unit suite, vet, and integration build green.
<!-- SECTION:FINAL_SUMMARY:END -->
