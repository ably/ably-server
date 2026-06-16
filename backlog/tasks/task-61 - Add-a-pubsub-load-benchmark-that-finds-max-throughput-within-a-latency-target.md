---
id: TASK-61
title: Add a pubsub load benchmark that finds max throughput within a latency target
status: To Do
assignee: []
created_date: '2026-06-16 18:14'
labels:
  - performance
dependencies: []
ordinal: 61000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Add a benchmark program that drives pub/sub load against a running ably-server (single node or the 3-node Compose cluster), verifies correctness of delivery, measures end-to-end latency, and reports the throughput achievable while staying within a desired p50/p99 latency budget.

Why: we have no way today to quantify performance or catch regressions. This gives a repeatable number ("X msgs/s within p99 < N ms") for the cluster topology defined in the Docker Compose task.

Behaviour:
- Spin up publishers and subscribers (via ably-go or the wire protocol) attaching to channels and publishing messages at a controlled rate.
- Correctness: every published message is received by all subscribers exactly once, in order per channel, with no gaps or duplicates — report any loss/dup/reorder as a failure.
- Latency: stamp publish time and measure delivery latency end-to-end; compute p50/p99 (and max).
- Throughput search: ramp/binary-search the offered load and report the highest sustained throughput that keeps p50/p99 under the configured target, rather than just running at one fixed rate.
- Output a clear summary: achieved throughput, p50/p99/max latency, correctness result.

Pointers: build alongside `cmd/` (e.g. `cmd/ably-bench`); cluster wiring mirrors `cmd/ably-server/integration_test.go`; depends on the Compose stack for a realistic multi-node target.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Generates configurable pubsub load (channels, publishers, subscribers, message rate/size) against a running server or the Compose cluster
- [ ] #2 Verifies correctness: detects and reports any message loss, duplication, or per-channel reordering
- [ ] #3 Measures end-to-end latency and reports p50/p99/max
- [ ] #4 Searches offered load and reports the max sustained throughput that stays within the configured p50/p99 target
<!-- AC:END -->
