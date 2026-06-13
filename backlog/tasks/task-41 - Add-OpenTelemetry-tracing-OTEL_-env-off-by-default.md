---
id: TASK-41
title: 'Add OpenTelemetry tracing (OTEL_* env, off by default)'
status: To Do
assignee: []
created_date: '2026-06-13 08:41'
labels:
  - ops
dependencies: []
ordinal: 41000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
DESIGN.md §10 specifies OpenTelemetry tracing, off by default and enabled via standard OTEL_* environment variables. Not implemented. Add optional OTEL trace export covering the main request paths.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 With no OTEL_* configuration present, tracing is off and adds no export overhead
- [ ] #2 Standard OTEL_* env vars enable and configure the trace exporter
- [ ] #3 WebSocket connection lifecycle, REST handlers, and the publish path emit spans when tracing is enabled
<!-- AC:END -->
