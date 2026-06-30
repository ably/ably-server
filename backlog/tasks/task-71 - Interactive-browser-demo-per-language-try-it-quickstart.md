---
id: TASK-71
title: Interactive browser demo + per-language try-it quickstart
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
ordinal: 71000
---

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 a browser page served BY each embed app shows live realtime through the embedded endpoint (ably-js)
- [x] #2 each language has one command to start the embedded server and one command/script to try it
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Reusable browser demo (demo/index.html; unmodified ably-js from CDN, connects to its own origin) served at /demo/ by every track. Verified live in a real browser: CONNECTED + publish round-trips into the feed, no console errors — through the Go in-process mount AND the Node Express reverse proxy. Per-language run + try-it commands in USING.md (Quick start table).
<!-- SECTION:FINAL_SUMMARY:END -->
