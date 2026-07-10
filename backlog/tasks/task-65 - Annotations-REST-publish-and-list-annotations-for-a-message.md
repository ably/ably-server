---
id: TASK-65
title: 'Annotations REST: publish and list annotations for a message'
status: To Do
assignee: []
created_date: '2026-07-09 11:05'
updated_date: '2026-07-10 10:13'
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
- [ ] #1 POST /channels/{channel}/messages/{serial}/annotations publishes an annotation and returns the assigned serial
- [ ] #2 GET /channels/{channel}/messages/{serial}/annotations returns the message's annotations, paginated via Link headers
- [ ] #3 Both endpoints accept and return JSON and msgpack
- [ ] #4 DESIGN.md §2.2 REST table updated
- [ ] #5 Auth matches DESIGN.md §14.4/§14.5: POST requires annotation-publish, GET requires history
<!-- AC:END -->
