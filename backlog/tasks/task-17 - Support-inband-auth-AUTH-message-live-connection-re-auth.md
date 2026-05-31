---
id: TASK-17
title: Support inband auth (AUTH message / live-connection re-auth)
status: To Do
assignee: []
created_date: '2026-05-31 16:11'
updated_date: '2026-05-31 16:11'
labels: []
dependencies:
  - TASK-9
ordinal: 17000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Implement inband re-authentication on an established WebSocket per Ably's AUTH flow. As a JWT token nears expiry, the server sends an AUTH message prompting the client to supply a fresh token. Handle the incoming AUTH message by: verifying the new token, checking the new credentials are compatible with the existing connection (same key / clientId constraints, capability), resetting the expiry timer to the new exp, and continuing the connection without disconnecting. On incompatible or invalid credentials, fail per protocol (ERROR / DISCONNECTED). Depends on JWT auth.
<!-- SECTION:DESCRIPTION:END -->
