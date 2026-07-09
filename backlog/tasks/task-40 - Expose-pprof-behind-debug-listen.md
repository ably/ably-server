---
id: TASK-40
title: Expose pprof behind --debug-listen
status: Done
assignee:
  - '@claude'
created_date: '2026-06-13 08:41'
updated_date: '2026-07-09 11:30'
labels:
  - ops
dependencies: []
ordinal: 40000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
DESIGN.md §10 specifies Go pprof served on a separate port behind a --debug-listen flag. Not implemented. Serve net/http/pprof on a dedicated listener when the flag is set.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 --debug-listen flag exists with an ABLY_SERVER_* env equivalent
- [x] #2 When set, the standard net/http/pprof endpoints are served on that separate address
- [x] #3 When unset, no debug/pprof listener is started
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Add --debug-listen flag (env ABLY_SERVER_DEBUG_LISTEN), empty default. When set, start a second net/http server on that address serving net/http/pprof's default handlers (import net/http/pprof for its DefaultServeMux registrations, then wrap in a dedicated mux/server), run it in a goroutine alongside the main listener, and shut it down during the graceful-shutdown sequence. When unset, no debug listener starts. Add tests: flag/env wiring exists and unset means no listener attempt (avoid asserting network behaviour where possible, or use a real ephemeral :0 listener check similar to Ready channel pattern).
<!-- SECTION:PLAN:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Added --debug-listen flag (env ABLY_SERVER_DEBUG_LISTEN, empty/disabled by default). When set, main.go starts a second http.Server on that address serving net/http/pprof's handlers (registered on http.DefaultServeMux via a blank import), torn down alongside the main server during graceful shutdown. When unset, no listener is created. Added a DebugReady test hook to runOpts (mirrors Ready) and unit tests covering both the enabled (pprof endpoint reachable) and disabled (no listener started) cases.
<!-- SECTION:FINAL_SUMMARY:END -->
