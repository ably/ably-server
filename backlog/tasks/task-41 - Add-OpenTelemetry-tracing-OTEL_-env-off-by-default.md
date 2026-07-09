---
id: TASK-41
title: 'Add OpenTelemetry tracing (OTEL_* env, off by default)'
status: Done
assignee:
  - '@claude'
created_date: '2026-06-13 08:41'
updated_date: '2026-07-09 14:05'
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
- [x] #1 With no OTEL_* configuration present, tracing is off and adds no export overhead
- [x] #2 Standard OTEL_* env vars enable and configure the trace exporter
- [x] #3 WebSocket connection lifecycle, REST handlers, and the publish path emit spans when tracing is enabled
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add internal/tracing package: Setup(ctx, getenv) (*Provider, error). Off by default — tracing is enabled only when an OTLP endpoint (OTEL_EXPORTER_OTLP_ENDPOINT / OTEL_EXPORTER_OTLP_TRACES_ENDPOINT) is set or OTEL_TRACES_EXPORTER names a non-none exporter, and never when OTEL_SDK_DISABLED=true or OTEL_TRACES_EXPORTER=none. When disabled: no exporter, no goroutines, Tracer is a no-op, Shutdown is a no-op, no global provider installed. When enabled: build the OTLP/HTTP trace exporter (otlptracehttp — reads standard OTEL_EXPORTER_OTLP_* envs), resource honouring OTEL_SERVICE_NAME (default ably-server), sdktrace TracerProvider with batcher, set global provider + W3C propagators.
   Dependency choice: OTLP exporter (otlptracehttp) rather than contrib/autoexport — smallest dep set honouring the standard envs; noted per task guidance.
2. Thread a trace.Tracer into realtime.Server + rest.Server (nil when disabled -> zero span overhead, no allocations on the hot path). Spans: ws.connection (connection lifecycle) with a child publish span on the WS publish worker; channel.publish child span on REST publish. otelhttp wraps the mux (only when enabled) for REST/HTTP request spans.
3. main.go run(): call tracing.Setup early, defer Shutdown, pass tracer to both servers, wrap handler with otelhttp when enabled.
4. Tests: internal/tracing unit test — default env => disabled, no-op tracer, non-recording spans; OTEL_* env => enabled, recording spans, Shutdown works. Realtime test injecting an sdktrace InMemoryExporter tracer, driving connect+publish, asserting ws.connection + publish spans recorded.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Dependency choice: used the OTLP/HTTP trace exporter (go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp) plus otel/sdk and contrib/otelhttp, rather than contrib/exporters/autoexport. This is the smallest dependency set that still honours the standard OTEL_EXPORTER_OTLP_* variables (the exporter reads endpoint/headers/protocol/TLS from the env itself). Enablement gate: on unless OTEL_SDK_DISABLED=true or OTEL_TRACES_EXPORTER=none, and requires either an OTLP endpoint env or an explicit OTEL_TRACES_EXPORTER. When disabled the global provider is left as the SDK no-op, no exporter/goroutine is created, and servers are handed a nil tracer so no spans (or context allocations) occur on the hot path. Only OTLP export is wired (console/other exporters not supported).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Add optional OpenTelemetry trace export configured via the standard OTEL_* env vars (DESIGN §10). New internal/tracing package: Setup inspects OTEL_SDK_DISABLED / OTEL_TRACES_EXPORTER / OTEL_EXPORTER_OTLP_[TRACES_]ENDPOINT; when off it installs no exporter, starts no goroutine, and returns a no-op tracer; when on it builds an OTLP/HTTP exporter (honouring the standard OTEL_EXPORTER_OTLP_* + OTEL_SERVICE_NAME envs), a batching TracerProvider, and W3C propagators. main.go calls Setup early, defers a bounded Shutdown, hands the servers a tracer only when enabled (nil otherwise -> zero hot-path overhead), and wraps the mux with otelhttp only when enabled. Spans: ws.connection (WS connection lifecycle) with a nested publish span on the WS publish worker; channel.publish on REST publish; otelhttp server spans for REST. Dependency: OTLP/HTTP exporter + otel/sdk + otelhttp (smallest set honouring the standard envs; autoexport avoided). Tests: internal/tracing unit tests (enablement table, disabled default => no-op non-recording spans, enabled => recording + clean Shutdown) and a realtime test injecting an in-memory exporter that asserts ws.connection + publish spans are emitted on connect+publish.
<!-- SECTION:FINAL_SUMMARY:END -->
