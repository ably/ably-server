---
id: TASK-27
title: Add process-wide Prometheus metrics
status: To Do
assignee: []
created_date: '2026-05-31 17:18'
updated_date: '2026-06-03 13:06'
labels:
  - ops
dependencies: []
ordinal: 27000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Expose Prometheus metrics at GET /metrics (DESIGN §10). Start with process-wide counters: connections opened (WS upgrades), HTTP requests per route (labelled by route + method + status), messages published (inbound publishes accepted), and messages sent to subscribers (outbound MESSAGE frames / ChannelMessages forwarded to attachments). Wire instrumentation into the realtime connection lifecycle (internal/realtime), the REST handlers (internal/rest), and the publish/forward path, and register the /metrics handler on the mux in cmd/ably-server/main.go. Keep labels low-cardinality here — no per-channel or per-connection labels (that is a separate task). DESIGN §10 also lists attach counters and connection-lifetime / publish-latency histograms; those can follow.
<!-- SECTION:DESCRIPTION:END -->
