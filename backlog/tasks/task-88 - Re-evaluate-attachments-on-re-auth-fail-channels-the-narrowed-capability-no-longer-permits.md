---
id: TASK-88
title: >-
  Re-evaluate attachments on re-auth: fail channels the narrowed capability no
  longer permits
status: To Do
assignee: []
created_date: '2026-07-09 20:59'
updated_date: '2026-07-12 16:37'
labels:
  - auth
  - realtime
  - compat
dependencies:
  - TASK-78
priority: low
ordinal: 88000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Found during TASK-78: the RTC8a1 'capabilities downgrade' subtest expects that when an inband re-auth (AUTH) narrows the connection's capability, an attached channel whose modes are no longer permitted moves to FAILED (channel-level ERROR with 40160). The per-operation capability enforcement from TASK-12 landed, and re-auth swaps the connection's capability set (TASK-17) — but existing attachments are never re-evaluated against the new capability. On successful re-auth, recompute each attachment's permitted modes; where the effective set becomes empty, detach the attachment and send the channel ERROR the SDK expects (check ably-go/the reference for exact frame + code). Where modes merely narrow, decide against the reference whether to renegotiate flags or leave the attachment (match reference behaviour). Verify with scripts/ably-server-compat.sh -r RTC8a in the ably-go checkout (read-only).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 After a re-auth that removes a channel's capability, the attached channel receives the protocol-correct failure and is detached server-side
- [ ] #2 The RTC8a1 capabilities-downgrade subtest passes against a local server
- [ ] #3 Unit test in internal/realtime pins the downgrade behaviour
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Per Lewis (2026-07-12): out of scope for the experimental release.
<!-- SECTION:NOTES:END -->
