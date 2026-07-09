---
id: TASK-12
title: Enforce capabilities per DESIGN.md §3.1
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 16:05'
updated_date: '2026-07-09 13:13'
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

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. internal/auth/capability.go: Op type + 8 op constants (incl 4 message-*); Capability{resource->opset}; ParseCapability(JSON); AllowAllCapability(); matchResource mirroring reference pathsMatch (whole-segment wildcards, trailing * matches >=0 trailing segments, interior * exactly one, foo* literal); Permits(channel,op) = union of matching resources' ops (with * meaning any op); Intersect() mirroring reference intersectPath+op-AND (preserving * op); canonical MarshalJSON/String. Table-driven tests for every §3.1 example.
2. Principal gains resolved capability (Basic/absent-claim -> AllowAll, token claim -> parsed; malformed claim -> ErrInvalidToken) exposed via Capabilities().
3. WS ATTACH: effective = requested ∩ capability-permitted modes; empty -> ERROR 40160, no attach. newAttachment takes resolved modes.
4. WS inbound MESSAGE create -> require publish cap (NACK 40160). PRESENCE already gated via PRESENCE-mode attachment (capability-derived).
5. REST: POST->publish, GET history/message/versions->history, GET presence->subscribe, GET presence/history->history; insufficient -> 401 Ably error shape (40160). Mutation (PATCH / WS mutation) gating deferred to TASK-51.
6. §3.3 narrowing: MintToken narrows requested capability against signing key's (full) capability. Update DESIGN §3.3 blockquote + stale TASK-12 doc notes.
<!-- SECTION:PLAN:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Enforce capabilities per DESIGN §3.1 (TASK-12). New internal/auth/capability.go implements Ably wildcard resource matching (whole-segment wildcards, trailing * matches >=0 trailing segments, interior * exactly one, literal foo*), per-op union across matching resources, and capability intersection mirroring the reference. Principal now carries a resolved capability set (Basic auth / absent claim -> full {"*":["*"]}, otherwise the parsed x-ably-capability claim; a malformed claim is rejected). WS ATTACH resolves effective modes = requested ∩ capability-permitted (subscribe->SUBSCRIBE+PRESENCE_SUBSCRIBE, publish->PUBLISH, presence->PRESENCE); empty intersection -> ERROR 40160 and no attach; ATTACHED.flags now carries the effective mode set (§4.2). Inbound WS MESSAGE create requires publish (NACK 40160); inbound PRESENCE is gated transitively by the capability-derived PRESENCE attach mode. REST endpoints enforce the op table (POST->publish, GET history/message/versions->history, GET presence->subscribe, GET presence/history->history) with 401 + Ably error shape (40160). MintToken narrows the requested capability against the signing key's (full) capability (§3.3); DESIGN §3.3 blockquote and stale TASK-12 notes updated. Table-driven tests cover every §3.1 wildcard/union/intersection example plus REST and realtime enforcement. Mutation (PATCH / WS mutation) capability gating is TASK-51.
<!-- SECTION:FINAL_SUMMARY:END -->
