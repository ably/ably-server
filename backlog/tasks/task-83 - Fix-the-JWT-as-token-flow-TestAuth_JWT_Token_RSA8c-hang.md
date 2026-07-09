---
id: TASK-83
title: Fix the JWT-as-token flow (TestAuth_JWT_Token_RSA8c hang)
status: To Do
assignee: []
created_date: '2026-07-09 19:22'
labels:
  - auth
  - compat
dependencies: []
priority: medium
ordinal: 83000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
COMPAT_REPORT_2026-07-09.md gap 5: the test mints an HS256 JWT signed with the key secret (via ably's echo server) and presents it as a token; the run hangs/panics. HS256 JWT verification is implemented (TASK-9), so diagnose the actual failure: whether the echo-server dependency makes this a harness artifact, whether the token transport (Authorization: Bearer base64(jwt) vs raw, ?access_token=) diverges, or whether verification rejects a claim shape the SDK produces. Fix server-side divergences; if the residual is purely the external echo-server dependency, document it as a harness artifact in the task.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The failure mode is diagnosed and recorded
- [ ] #2 TestAuth_JWT_Token_RSA8c passes, or its failure is documented as a harness artifact with the server side verified correct
<!-- AC:END -->
