---
id: TASK-5
title: Support multiple API keys (at least one required)
status: To Do
assignee: []
created_date: '2026-05-31 16:05'
labels: []
dependencies: []
ordinal: 5000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Today a single API key is configured via --api-key / ABLY_SERVER_API_KEY and internal/auth compares against exactly one key. Support configuring multiple keys (repeatable --api-key flag and/or comma-separated env), requiring at least one (startup fails with none). The Authenticator should accept a request matching ANY configured key (basic auth / ?key=). This is foundational for JWT auth, which selects the signing key by the JWT kid header. Update DESIGN.md §3 (currently "A single API key").
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 At least one API key is required; startup fails if none is provided
- [ ] #2 A request matching any configured key authenticates successfully
<!-- AC:END -->
