---
id: TASK-42
title: Add presence protocol types and channel-mode flags
status: Done
assignee:
  - '@lmars'
created_date: '2026-06-13 09:37'
updated_date: '2026-06-13 15:20'
labels:
  - presence
dependencies: []
documentation:
  - DESIGN.md
ordinal: 42000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Add the wire types presence needs, per DESIGN.md section 12.1 and section 8. Define protocol.PresenceMessage (ID is the optional client-supplied idempotency key, Serial is the server-assigned channelSerial:idx, plus Action, ClientID, ConnectionID, Data, Encoding, Timestamp) and a PresenceAction enum (ABSENT 0, PRESENT 1, ENTER 2, LEAVE 3, UPDATE 4). Add a Presence slice to protocol.ChannelMessage so a ChannelMessage carries either Messages or Presence. Add the channel-mode flag constants PRESENCE (1<<16), PUBLISH (1<<17), SUBSCRIBE (1<<18), PRESENCE_SUBSCRIBE (1<<19) and the HAS_PRESENCE attach flag (1<<0); RESUMED (1<<2) already exists. The PRESENCE (14) and SYNC (16) actions already exist in the action enum. This is the foundation the storage and realtime presence work builds on.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 protocol.PresenceMessage is defined with fields ID, Serial, Action, ClientID, ConnectionID, Data, Encoding, Timestamp and json+msgpack tags following the Message convention
- [x] #2 PresenceAction enum defined: ABSENT=0, PRESENT=1, ENTER=2, LEAVE=3, UPDATE=4, with a String() method
- [x] #3 protocol.ChannelMessage gains Presence []*PresenceMessage (omitempty); a ChannelMessage carries either Messages or Presence, never both
- [x] #4 Channel-mode flag constants added: PRESENCE=1<<16, PUBLISH=1<<17, SUBSCRIBE=1<<18, PRESENCE_SUBSCRIBE=1<<19; attach flag HAS_PRESENCE=1<<0
- [x] #5 PresenceMessage and a presence-bearing ChannelMessage round-trip through both the JSON and msgpack codecs (encode then decode is equal), covered by tests
- [x] #6 No behavioural change to existing message/encoding paths; the existing test suite still passes
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. internal/protocol/presence.go: PresenceMessage struct (ID, Serial, Action, ClientID, ConnectionID, Data, Encoding, Timestamp) + PresenceAction enum (absent/present/enter/leave/update) with String().
2. message.go: add Presence []*PresenceMessage to ChannelMessage and ProtocolMessage (omitempty); add flag consts HAS_PRESENCE=1<<0, PRESENCE=1<<16, PUBLISH=1<<17, SUBSCRIBE=1<<18, PRESENCE_SUBSCRIBE=1<<19.
3. presence_test.go: round-trip PresenceMessage + presence-bearing ChannelMessage/ProtocolMessage through JSON and msgpack. Message action set is out of scope (TASK-49).
<!-- SECTION:PLAN:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Added the presence wire types (DESIGN.md §12.1, §8).

- internal/protocol/presence.go: PresenceMessage (ID/Serial split mirroring Message, plus Action, ClientID, ConnectionID, Data, Encoding, Timestamp) and the PresenceAction enum (absent/present/enter/leave/update) with String().
- ChannelMessage and ProtocolMessage each gain Presence []*PresenceMessage (a cm carries Messages XOR Presence).
- Channel-mode flag constants: HAS_PRESENCE=1<<0, PRESENCE=1<<16, PUBLISH=1<<17, SUBSCRIBE=1<<18, PRESENCE_SUBSCRIBE=1<<19.

Round-trip tests cover PRESENCE/SYNC frames through JSON and msgpack. The Message action set (create/update/delete/append) is deferred to TASK-49 (mutable messages). Tests: go test ./internal/protocol/... and the full unit suite are green.
<!-- SECTION:FINAL_SUMMARY:END -->
