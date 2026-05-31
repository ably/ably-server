---
id: TASK-21
title: Add PostgreSQL storage implementation + schema
status: To Do
assignee: []
created_date: '2026-05-31 16:27'
updated_date: '2026-05-31 16:28'
labels: []
dependencies:
  - TASK-13
ordinal: 21000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Implement the storage.Storage / storage.ChannelStore interfaces for PostgreSQL (cluster mode, DESIGN §6.3) in a new internal/storage/postgres package. Create the schema per the §6.3 sketch: channels; channel_messages (PK (channel, channel_serial), one row per atomic publish); messages (PK (channel, channel_serial, idx), FK -> channel_messages ON DELETE CASCADE, plus a unique index on (channel, id) WHERE id IS NOT NULL for idempotency). AppendChannelMessage persists one ChannelMessage + its Messages atomically and enforces id-based idempotency via the unique index; History runs a bounded forward range scan ordered by channel_serial. Mint channelSerials in the backend and persist generator monotonic state across restarts (the Postgres equivalent of the bbolt _meta state, DESIGN §8). Exercise it against the shared contract suite in internal/storage/storagetest. Retention (TTL expiry) and the per-channel message cap are handled as a cross-backend concern in a separate task. Depends on the serial-generation consolidation (storage owns minting).
<!-- SECTION:DESCRIPTION:END -->
