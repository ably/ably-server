---
id: TASK-26
title: Decide how to handle retention and max channel length
status: To Do
assignee: []
created_date: '2026-05-31 16:31'
updated_date: '2026-06-03 13:06'
labels:
  - scoping
dependencies: []
priority: low
ordinal: 26000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Decide and document a consistent strategy for message retention (TTL expiry) and the per-channel message cap (max channel length) that applies across ALL storage backends — memory, bbolt, and PostgreSQL — rather than being re-invented per backend. DESIGN §6 sets defaults (message TTL 2m, max-messages-per-channel 1000, both configurable) and sketches per-backend mechanisms (memory ring buffer; bbolt background sweep over serial-ordered keys; Postgres periodic delete job), but the cross-cutting questions are open: where the policy lives (shared logic vs the storage interface vs per-backend), how the cap is counted (ChannelMessages vs Messages), how expiry interacts with the in-process Channel linked list and live attachments (DESIGN §5.1), and how the idempotency-id index is trimmed alongside messages.

Output: a short decision (and any DESIGN.md update) the per-backend implementations can follow. Not a top priority — it informs, but should not block, the individual backend work.
<!-- SECTION:DESCRIPTION:END -->
