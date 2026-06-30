---
id: TASK-73
title: >-
  Cross-language run-it/use-it doc + why-embedded value-prop page (dev vs
  production packaging)
status: Done
assignee:
  - '@claude'
created_date: '2026-06-30 20:43'
updated_date: '2026-06-30 21:36'
labels:
  - embedding-poc
dependencies: []
references:
  - EMBEDDING-POC.md
ordinal: 73000
---

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 per-language: dev/local build+link+run AND production package shape (npm optionalDeps, NuGet rid, PyPI wheels, Go module)
- [x] #2 value-prop page: no deps, no separate server, just an endpoint on your web server; links the live demo + try-it
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
experiments/embedding/USING.md: why-embedded value prop, per-language quick start (run + try-it), dedicated-port model, dev/local vs production packaging (npm optionalDeps / NuGet rid / PyPI wheels / Go module), root vs subpath, auth guidance, a 'When NOT to embed' section (serverless/edge, multi-instance shared state, no-exec FS, WSGI, friction points) and a per-language sweet-spot table. RESULTS.md gained a Prior-art section and a decision boundary; both updated to include the Python track.
<!-- SECTION:FINAL_SUMMARY:END -->
