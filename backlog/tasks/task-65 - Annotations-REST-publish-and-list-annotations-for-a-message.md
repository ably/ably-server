---
id: TASK-65
title: 'Annotations REST: publish and list annotations for a message'
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 11:05'
updated_date: '2026-07-10 10:38'
labels: []
dependencies:
  - TASK-63
priority: high
ordinal: 65000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
REST surface for annotations per Ably's API: POST /channels/{channel}/messages/{serial}/annotations publishes an annotation (create or delete per body action) and GET on the same path returns the annotations for that message, paginated with the standard Link header convention. Gate on the annotation-publish capability op for writes; reads follow Ably's op mapping. Reference realtime roles/frontdoor/transports/rest/annotations.go.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 POST /channels/{channel}/messages/{serial}/annotations publishes an annotation and returns the assigned serial
- [x] #2 GET /channels/{channel}/messages/{serial}/annotations returns the message's annotations, paginated via Link headers
- [x] #3 Both endpoints accept and return JSON and msgpack
- [x] #4 DESIGN.md §2.2 REST table updated
- [x] #5 Auth matches DESIGN.md §14.4/§14.5: POST requires annotation-publish, GET requires history
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
rest/server.go: HandlePublishAnnotation (POST /channels/{name}/messages/{serial}/annotations) - authenticate + resolveRequestClientID + authorize annotation-publish; parse single-or-array annotation body (JSON/msgpack); force each MessageSerial = path serial; resolve/stamp clientId (MessageClientID) + timestamp; Validate (anonymous multiple/total exception); ch.PublishAnnotation; 201 {channel,serial,serials}; ErrTargetNotFound -> 40400; validation -> 400. HandleListAnnotations (GET same path) - authorize history; parse history query, default direction forwards (stream order like versions); ch.Annotations(serial,q); flatten -> []*Annotation JSON/msgpack; writeLinkHeaders with last annotation serial cursor. Helpers: parseAnnotations, marshalAnnotations, flattenAnnotations, lastAnnotationSerial. Wire routes in cmd/ably-server newMux (instrumented) + rest test-server mux. §2.2 table already lists both endpoints. Tests: publish+list round-trip (json+msgpack), pagination Link, auth (annotation-publish for POST, history for GET), unknown target 404.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
HandlePublishAnnotation (POST) + HandleListAnnotations (GET) added to rest/server.go and wired into cmd/ably-server newMux (instrumented) and the rest test-server mux. POST: authenticate + resolveRequestClientID + authorize annotation-publish; parse single-or-array body (JSON/msgpack); path serial is authoritative (overrides body messageSerial); resolve/stamp clientId + timestamp; Annotation.Validate (anonymous multiple/total exception -> 400); ch.PublishAnnotation; 201 {channel,serial,serials}; ErrTargetNotFound -> 404 (40400). GET: authorize history; default direction forwards (stream order, like versions); flatten to []*Annotation JSON/msgpack; per-line relative Link rel=next via writeLinkHeaders keyed on the last annotation serial. §2.2 table already listed both endpoints (added when §14 was written) so no change needed there. Tests: JSON+msgpack publish/list round-trip, Link pagination walk, unknown-target 404, capability enforcement (POST needs annotation-publish, GET needs history), anonymous distinct.v1 rejected / multiple.v1 allowed.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
REST annotations: POST /channels/{channel}/messages/{serial}/annotations publishes (annotation-publish cap, clientId/anonymous-method rules, target from path, JSON+msgpack, 404 on unknown target) and GET lists a message's annotations in stream order with the standard relative Link rel=next pagination (history cap). Routes wired into newMux and the rest test mux. Response shapes mirror the reference. All unit tests pass.
<!-- SECTION:FINAL_SUMMARY:END -->
