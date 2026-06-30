---
id: TASK-63
title: >-
  Embedding conformance harness (pub/sub, history, resume) runnable against any
  host:port
status: Done
assignee:
  - '@claude'
created_date: '2026-06-30 16:31'
updated_date: '2026-06-30 16:32'
labels:
  - embedding-poc
dependencies: []
references:
  - EMBEDDING-POC.md
ordinal: 63000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Language-agnostic scenarios + runner. Baseline run against a standalone ably-server binary.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 harness drives publish, subscribe, history and resume-after-drop
- [x] #2 passes against a plain ably-server --mode memory binary (baseline numbers recorded)
- [x] #3 confirms whether a non-WebSocket transport exists; scopes reliability leg accordingly
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Build Go harness using ably-go SDK for pubsub/restpubsub/history + raw WS for deterministic resume. 2. Run against standalone memory-mode binary. 3. Probe transports. 4. Record measured baseline.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Harness at experiments/embedding/harness (in-module so resume can use internal/protocol). Six scenarios: connect, pubsub, restpubsub, history, resume, soak. Baseline PASSES all six against ably-server --mode memory; report saved to experiments/embedding/results/baseline.json. Negative test (closed port) correctly fails, so PASS is meaningful.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Built a language-agnostic conformance harness (Go) runnable against any Ably endpoint by host+port, and set the PoC floor against a standalone memory-mode ably-server.

Scenarios: connect, pubsub (SDK round-trip latency), restpubsub (REST->WS cross-protocol), history (SDK REST publish + ordered read), resume (raw-WS deterministic resume-after-drop asserting RESUMED flag + exact gap replay), soak (zero-loss).

Baseline (loopback) PASS: connect 2.44ms; pubsub mean 0.15ms/p99 0.21ms; restpubsub mean 0.27ms; history 10 ordered; resume 3/3 gap delivered, RESUMED set; soak 200 msgs 0 lost ~2170 msg/s.

Transport finding: ably-server exposes WebSocket (GET /) + REST only; no comet/SSE (/comet,/sse return 404). Reliability cross-transport leg scoped to WS+REST. Binary ~15.5MB arm64.
<!-- SECTION:FINAL_SUMMARY:END -->
