---
id: TASK-64
title: 'M1: Go in-process embed example + harness run (DX ceiling)'
status: Done
assignee:
  - '@claude'
created_date: '2026-06-30 16:31'
updated_date: '2026-06-30 16:38'
labels:
  - embedding-poc
dependencies: []
references:
  - EMBEDDING-POC.md
ordinal: 64000
---

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 ably-server handlers mounted in-process in a tiny Go app (no child process)
- [x] #2 harness passes through it; metrics recorded
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
ablyembed package mounts internal/core+realtime+rest+storage/memory as an http.Handler (same wiring as cmd/ably-server, no server change). Example host app = ~6 lines of glue. Harness PASSES all six through the in-process mount (JSON + msgpack). Graceful shutdown clean. Host-app binary 9.1MB (memory-only) vs 15.5MB standalone. No child = zero supervision but shared blast radius.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Go in-process embed (the DX/overhead ceiling). A host app imports ablyembed, calls New, and serves the returned http.Handler — no child process, no proxy, no FFI.

Result: harness PASSES all six scenarios through the in-process mount (connect 1.63ms; pubsub mean 0.11ms; restpubsub mean 0.29ms; history ordered; resume RESUMED + 3/3 gap lossless; soak 200 msgs 0 loss ~2247 msg/s). msgpack also passes. Graceful shutdown exits 0.

Glue: 3 essential lines (New, mount Handler, defer Close). Binary: 9.1MB host app (memory-only) vs 15.5MB standalone. Trade-off: zero supervision burden, but in-process shared blast radius (a server panic takes the app down). Files: experiments/embedding/go/{ablyembed,example}, NOTES.md, results/go-inproc.json.
<!-- SECTION:FINAL_SUMMARY:END -->
