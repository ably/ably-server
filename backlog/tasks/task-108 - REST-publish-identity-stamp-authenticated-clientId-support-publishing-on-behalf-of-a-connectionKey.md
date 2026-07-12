---
id: TASK-108
title: >-
  REST publish identity: stamp authenticated clientId; support publishing on
  behalf of a connectionKey
status: To Do
assignee: []
created_date: '2026-07-12 13:59'
labels:
  - compat
  - ably-js
dependencies: []
priority: medium
ordinal: 108000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Two identity gaps on REST publish (rest/message, 2026-07-12 run): (1) 'Should implicitly send clientId when authenticated with clientId' — a REST client whose auth carries a clientId publishes without an explicit message clientId; Ably stamps the authenticated clientId on the stored message (the SDK deliberately does NOT send it, RSL1m1). Our REST publish path doesn't stamp it, so history returns the message without clientId. The realtime path already resolves identity via auth.MessageClientID — mirror that in REST publish. (2) 'allows you to publish a message on behalf of a Realtime connection by setting connectionKey on the message' — Ably resolves message.connectionKey to the target connection and stamps its connectionId ('expected undefined to equal <connectionId>'). Needs the wire field on Message plus a connectionKey->connection lookup (single-node: the in-process registry; cluster mode can 40006 unknown keys or route via the broker — decide in implementation).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 REST publish with clientId-bearing auth stamps that clientId on the stored message
- [ ] #2 REST publish with message.connectionKey stamps the target connection's connectionId (or errors per Ably for unknown keys)
- [ ] #3 The two rest/message identity tests pass
<!-- AC:END -->
