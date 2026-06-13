---
id: TASK-38
title: Wire --tls-cert/--tls-key for inline TLS
status: To Do
assignee: []
created_date: '2026-06-13 08:41'
labels:
  - config
dependencies: []
ordinal: 38000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
DESIGN.md §9 lists optional inline TLS via --tls-cert/--tls-key (with ABLY_SERVER_* env equivalents). These are neither defined nor wired in cmd/ably-server today; the server only serves plain HTTP. Serve HTTP/WS over TLS when both are supplied.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 --tls-cert and --tls-key flags exist, each with an ABLY_SERVER_* env equivalent
- [ ] #2 When both are set, the server serves HTTP and WebSocket traffic over TLS using the supplied cert/key
- [ ] #3 When neither is set, the server serves plain HTTP as it does today
- [ ] #4 Supplying only one of the pair is a startup error
<!-- AC:END -->
