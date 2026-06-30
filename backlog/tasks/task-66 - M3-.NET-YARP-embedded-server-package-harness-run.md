---
id: TASK-66
title: 'M3: .NET + YARP embedded-server package + harness run'
status: Done
assignee:
  - '@claude'
created_date: '2026-06-30 16:31'
updated_date: '2026-06-30 16:56'
labels:
  - embedding-poc
dependencies: []
references:
  - EMBEDDING-POC.md
ordinal: 66000
---

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Process supervision + YARP route with WebSocket proxying
- [x] #2 harness passes through an example .NET app; metrics recorded
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
YARP catch-all proxy to a supervised child ably-server. Independently re-verified by coordinator: harness PASS all six through YARP (:8571 fresh run); fault injection C1 (kill-9 child -> respawn -> harness passes) + C2 (SIGTERM -> exit 0, no orphan) PASS; native IO.Ably v1.2.18 realtime round-trip 6ms through YARP. Finding: IO.Ably blocks key-auth REST over plain http (EnsureSecureConnection, no opt-out); token-auth REST works.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Embedded ably-server in a .NET host via a supervised child process + YARP reverse proxy (WebSocket + REST) on a dedicated port.

Verified (coordinator re-ran from clean build): Go harness PASSES all six through YARP (connect ~5-10ms; pubsub mean ~0.2ms; resume RESUMED+lossless; soak 200 msgs 0-loss ~1800-2100 msg/s). Fault injection PASSES: kill-9 child auto-respawns and harness passes; SIGTERM exits 0 with no orphan. Native IO.Ably SDK realtime pub/sub works through YARP (CONNECTED 54ms, round-trip 6ms).

Finding: the IO.Ably SDK refuses key (basic-auth) REST over plain http via EnsureSecureConnection with no ClientOptions opt-out; token-auth REST works through YARP as a workaround. Realtime (the primary path) is unaffected. Files under experiments/embedding/dotnet; results in results/dotnet-*.{json,log}.
<!-- SECTION:FINAL_SUMMARY:END -->
