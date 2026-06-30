---
id: TASK-67
title: 'M4: Fill the embedding trade-off matrix (RESULTS.md) + recommendation'
status: Done
assignee:
  - '@claude'
created_date: '2026-06-30 16:31'
updated_date: '2026-06-30 19:03'
labels:
  - embedding-poc
dependencies: []
references:
  - EMBEDDING-POC.md
ordinal: 67000
---

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 all six dimensions measured for Go, Node and .NET
- [x] #2 RESULTS.md states a clear go/no-go recommendation per ecosystem
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Merged all three tracks into the PoC branch (clean, no conflicts). Re-ran baseline against the merged/fixed server; full go test ./... green. RESULTS.md fills the §7 matrix with measured numbers for floor, Go in-process, Node (Express+Fastify), .NET (YARP); documents the ably-js server fixes (TASK-68/69), the IO.Ably REST-over-http finding, transport coverage, the Express shutdown bug fixed, and a Python/Java feasibility assessment. Recommendation: ship; Node+Go first, .NET close behind.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Filled experiments/embedding/RESULTS.md with the measured §7 trade-off matrix across Go in-process (ceiling), Node Express/Fastify, .NET YARP, plus the standalone floor. All tracks pass the conformance harness 6/6 (pubsub, REST->WS, history, resume-after-drop, soak) and the fault-injection legs (kill -9 auto-restart; clean SIGTERM, no orphan).

Key findings: embedding overhead is low (single-digit-ms connect, sub-ms pub/sub through a proxy); unmodified ably-go works everywhere; ably-js required two real server fixes (connectionDetails CD2, msgSerial *int64 — TASK-68/69, implemented+verified); IO.Ably realtime works through YARP but refuses key-auth REST over plain http. Recommendation: productise embedded distribution — Node (primary) and Go first, .NET close behind; promote the two server fixes to mainline and add an SDK basePath option for same-origin mounting.
<!-- SECTION:FINAL_SUMMARY:END -->
