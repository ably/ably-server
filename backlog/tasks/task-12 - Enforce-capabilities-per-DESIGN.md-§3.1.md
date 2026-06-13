---
id: TASK-12
title: Enforce capabilities per DESIGN.md §3.1
status: To Do
assignee: []
created_date: '2026-05-31 16:05'
updated_date: '2026-06-03 13:06'
labels:
  - auth
dependencies:
  - TASK-9
ordinal: 12000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Implement capability resolution and enforcement per DESIGN.md §3/§3.1. Parse x-ably-capability ({resource: [ops]}) using Ably's wildcard semantics (whole-segment wildcards, a trailing `*` matching any number of trailing segments, literal `foo*`); default to the key's full capability {"*":["*"]} when the claim is absent. Compute, per op (publish / subscribe / history), the union of granted ops across matching resources and check the requested op. Enforce across: WS ATTACH flag resolution (effective modes = requested ∩ permitted; empty -> ERROR 40160, no attach), inbound WS MESSAGE (publish), REST publish (publish) and REST history (history). Depends on JWT auth (capabilities arrive via the claim).
<!-- SECTION:DESCRIPTION:END -->
