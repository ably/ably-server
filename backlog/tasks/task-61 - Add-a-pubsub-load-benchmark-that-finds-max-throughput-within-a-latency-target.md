---
id: TASK-61
title: Add a pubsub load benchmark that finds max throughput within a latency target
status: Done
assignee:
  - '@claude'
created_date: '2026-06-16 18:14'
updated_date: '2026-06-16 21:20'
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
- [x] #1 Generates configurable pubsub load (channels, publishers, subscribers, message rate/size) against a running server or the Compose cluster
- [x] #2 Verifies correctness: detects and reports any message loss, duplication, or per-channel reordering
- [x] #3 Measures end-to-end latency and reports p50/p99/max
- [x] #4 Searches offered load and reports the max sustained throughput that stays within the configured p50/p99 target
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Added cmd/ably-bench (main.go CLI+search+reporting, trial.go run/clients, stats.go histogram+payload+correctness). Uses ably-go realtime clients (same basic-auth/no-TLS wiring as the integration tests), x/time/rate to pace offered load, and PublishAsync with a bounded in-flight semaphore so a server that can't keep up shows as backpressure (achieved < offered) rather than unbounded memory. Each message embeds publisherID:seq:publishNanos+padding; the same process publishes and subscribes so latency is skew-free. Correctness is exact per-publisher (loss/dup/reorder) via monotonic seq tracking; latency via a 100µs-resolution histogram (p50/p99/max). --search ramps x2 to first budget breach then binary-searches for the max sustained rate within --p50/--p99.

Verified against the TASK-60 compose cluster and a single in-memory node. Findings (laptop, docker): in-memory mode sustained ~47k msg/s at p50=0.2ms/p99=1.1ms; cluster mode (Postgres LISTEN/NOTIFY) hit a hard knee around ~2-2.5k msg/s aggregate where latency climbs into the hundreds of ms / seconds while delivery stays correct. Same benchmark binary in both, so the ceiling is the cluster write/NOTIFY path, not the tool — worth a follow-up perf task.
<!-- SECTION:NOTES:END -->
