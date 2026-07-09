---
id: TASK-27
title: Add process-wide Prometheus metrics
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 17:18'
updated_date: '2026-07-09 13:58'
labels:
  - ops
dependencies: []
ordinal: 27000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Expose Prometheus metrics at GET /metrics (DESIGN §10). Start with process-wide counters: connections opened (WS upgrades), HTTP requests per route (labelled by route + method + status), messages published (inbound publishes accepted), and messages sent to subscribers (outbound MESSAGE frames / ChannelMessages forwarded to attachments). Wire instrumentation into the realtime connection lifecycle (internal/realtime), the REST handlers (internal/rest), and the publish/forward path, and register the /metrics handler on the mux in cmd/ably-server/main.go. Keep labels low-cardinality here — no per-channel or per-connection labels (that is a separate task). DESIGN §10 also lists attach counters and connection-lifetime / publish-latency histograms; those can follow.
<!-- SECTION:DESCRIPTION:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add internal/metrics package: Metrics struct wrapping a dedicated prometheus.Registry with process-wide, low-cardinality collectors (nil-receiver-safe methods so tests can pass nil):
   - ably_connections_opened_total (counter, WS upgrades)
   - ably_connections_open (gauge)
   - ably_connection_lifetime_seconds (histogram)
   - ably_attachments_total (counter)
   - ably_messages_published_total (counter, inbound accepted publishes)
   - ably_messages_delivered_total (counter, outbound MESSAGE frames forwarded)
   - ably_publish_latency_seconds (histogram, inbound publish -> storage commit)
   - ably_http_requests_total{route,method,status}
   Handler() returns promhttp handler over the registry.
2. Thread *metrics.Metrics into realtime.Server + rest.Server constructors (tests pass nil). Instrument: connection open/close+lifetime, attach, WS message publish (+latency), attachment.forward (delivered), REST publish (+latency).
3. main.go: build one Metrics, pass to both servers, register GET /metrics on main mux (unauth, like /healthz), wrap REST routes with an HTTP-metrics middleware capturing route pattern/method/status.
4. Update DESIGN §10 to enumerate actual metric names.
5. Test in cmd/ably-server: drive WS connect + publish + REST request through run(), scrape /metrics, assert series exist with plausible values.
<!-- SECTION:PLAN:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Expose process-wide Prometheus metrics at GET /metrics (unauthenticated, main listener, alongside /healthz). New internal/metrics package wraps a dedicated registry with low-cardinality series: ably_connections_opened_total, ably_connections_open, ably_connection_lifetime_seconds, ably_attachments_total, ably_messages_published_total, ably_messages_delivered_total, ably_publish_latency_seconds, and ably_http_requests_total{route,method,status}, plus the Go/process collectors. Instrumentation wired into the realtime connection lifecycle (open/close + lifetime, attach, WS publish + latency, MESSAGE forward) and the REST publish path; REST routes wrapped with an HTTP-metrics middleware keyed on the route pattern. Metrics methods are nil-receiver-safe so tests inject nil. DESIGN §10 now enumerates the metric names. Added a memory-mode cmd test that drives WS connect+attach, a REST publish, and a delivered MESSAGE, then scrapes /metrics and asserts the series. Dependency: github.com/prometheus/client_golang.
<!-- SECTION:FINAL_SUMMARY:END -->
