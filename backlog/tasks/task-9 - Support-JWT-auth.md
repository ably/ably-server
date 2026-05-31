---
id: TASK-9
title: Support JWT auth
status: To Do
assignee: []
created_date: '2026-05-31 16:05'
updated_date: '2026-05-31 16:11'
labels: []
dependencies:
  - TASK-5
ordinal: 9000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Add JWT bearer-token verification per DESIGN.md §3. Accept tokens via Authorization: Bearer <jwt> or ?accessToken=. Verify the HS256 signature against the configured key's secret, selecting the key by the JWT kid header (depends on multiple-API-keys support). Validate the required iat (with a small clock-skew leeway) and exp claims; surface auth failures as WS ERROR + close / REST 401. Parse the optional x-ably-capability and x-ably-clientId claims and make them available for downstream resolution (capability enforcement and clientId resolution are separate tasks). Implement in internal/auth alongside the existing key parsing.
<!-- SECTION:DESCRIPTION:END -->
