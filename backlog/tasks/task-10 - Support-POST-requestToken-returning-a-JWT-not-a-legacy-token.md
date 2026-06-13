---
id: TASK-10
title: Support POST /requestToken returning a JWT (not a legacy token)
status: To Do
assignee: []
created_date: '2026-05-31 16:05'
updated_date: '2026-06-03 13:06'
labels:
  - auth
dependencies:
  - TASK-9
ordinal: 10000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Implement a token-request endpoint that mints and returns a JWT (HS256, signed with the matching key's secret, kid header set) rather than an Ably legacy TokenRequest/TokenDetails. Honour the requested ttl, capability (-> x-ably-capability claim) and clientId (-> x-ably-clientId claim), constrained by the authenticating key's own capability. Returns the signed JWT for SDKs to use as accessToken. Depends on the JWT auth infrastructure. Note: confirm the exact path and request/response shape SDKs expect for a JWT-returning token endpoint before finalising.
<!-- SECTION:DESCRIPTION:END -->
