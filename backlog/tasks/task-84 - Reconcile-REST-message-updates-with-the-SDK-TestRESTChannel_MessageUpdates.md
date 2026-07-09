---
id: TASK-84
title: Reconcile REST message-updates with the SDK (TestRESTChannel_MessageUpdates)
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 19:22'
updated_date: '2026-07-09 22:23'
labels:
  - rest
  - compat
dependencies:
  - TASK-53
priority: high
ordinal: 84000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-09.md gap 6: the realtime message-updates path passes but TestRESTChannel_MessageUpdates fails, despite REST mutations landing in TASK-53 (PATCH /channels/{channel}/messages/{serial}). Diagnose what the SDK actually sends for REST update/delete/append (route, method, body shape, response envelope) and reconcile the implementation — the mismatch is likely route or payload shape rather than missing functionality.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 The SDK's REST mutation request shape is documented in the task notes
- [x] #2 TestRESTChannel_MessageUpdates passes against a local server
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Read the SDK's rest_channel.go to capture the exact request shapes for publish/update/delete/append/get/versions. 2. Diagnose the failure by booting the server and running the test verbose. 3. Fix the server wire shape (publish response + versions default ordering). 4. Pin with internal/rest unit tests; update DESIGN §2.2/§13.4. 5. Verify via harness + full local build/vet/test.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
## SDK REST mutation request shape (AC#1)

Read from ably-go (branch server-testing) ably/rest_channel.go. The REST client defaults to msgpack (Accept/Content-Type application/x-msgpack), JSON when NoBinaryProtocol.

- PublishWithResult / PublishMultipleWithResult: POST /channels/{name}/messages, body = JSON array (or single) of Messages. Response decoded into {"serials":["..."]} (publishResponse{Serials []string}); results[i].Serial = serials[i]. The serial is then used to address the message.
- UpdateMessage/DeleteMessage/AppendMessage (performMessageOperation): PATCH /channels/{name}/messages/{serial}. Target serial in the PATH; body = a SINGLE Message (not an array) carrying action (update=1/delete=2/append=5) + supplied fields + optional version {description, metadata}. Response decoded into {"versionSerial":"..."} (updateDeleteResult). Rejects a message with no serial client-side (40003, "lacks a serial").
- GetMessage: GET /channels/{name}/messages/{serial} -> single Message (latest version).
- GetMessageVersions(serial, nil): GET /channels/{name}/messages/{serial}/versions, NO direction param; paginated; expects create-first ordering (versions[0]=CREATE, then UPDATEs).

## Root cause

Not missing functionality; the PATCH/GET wire shapes already matched (TASK-53). Two shape mismatches:
1. Publish response: server returned {channel, messageId}; SDK reads {serials:[...]}. So PublishWithResult.Serial was nil, which cascaded into EVERY subtest (update/delete/append/get/versions all publish first and require the serial).
2. Versions default ordering: SDK's GetMessageVersions sends no direction; server defaulted to backwards (newest-first) like history, so versions[0] was the latest UPDATE, failing the create-first assertion.

## Fix (internal/rest/server.go)

1. publishResponse gains Serials []string (serials, omitempty, JSON+msgpack); HandlePublish populates it from each published cm.Messages[i].Serial (batch order). channel/messageId kept.
2. HandleMessageVersions defaults direction to forwards when no direction query param is present (explicit direction still wins).

Pinned by internal/rest unit tests (serials shape + addressability in server_test.go; versions default-forwards in mutation_test.go). DESIGN §2.2 (publish response body) and §13.4 (versions default ordering) updated.

## Verification
- Harness: TestRESTChannel_MessageUpdates PASS (all subtests). TestRealtimeChannel_MessageUpdates PASS (no regression). TestRESTChannel PASS (publish-shape regression check).
- Local: go build/vet (+ -tags=integration vet) and go test ./... all pass.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Reconciled REST message-updates with the SDK. Root cause was wire-shape, not missing functionality: the per-channel publish response returned {channel, messageId} but the SDK's PublishWithResult decodes {serials:[...]}, so the serial was nil and cascaded into every subtest; and the versions endpoint defaulted to backwards while the SDK (no direction param) expects create-first. Added Serials to the publish response (populated from each message's stable identity serial) and defaulted the versions read to forwards. Pinned both with internal/rest unit tests and updated DESIGN §2.2/§13.4. TestRESTChannel_MessageUpdates now passes via the harness; TestRealtimeChannel_MessageUpdates and TestRESTChannel unaffected.
<!-- SECTION:FINAL_SUMMARY:END -->
