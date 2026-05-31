---
id: TASK-22
title: Confirm and implement the Postgres LISTEN/NOTIFY cluster pub/sub broker
status: To Do
assignee: []
created_date: '2026-05-31 16:27'
updated_date: '2026-05-31 16:28'
labels: []
dependencies:
  - TASK-21
ordinal: 22000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Cross-node fan-out for cluster mode: a publish on any node must reach attachments on every other node. First confirm the approach in DESIGN §7.2 (and with the team if anything is unsettled), then implement it in internal/cluster.

Per §7.2: each node runs a single goroutine on a dedicated Postgres connection that LISTENs for channel-publish notifications. The publishing node INSERTs the channel_messages/messages rows (storage) and emits NOTIFY ably_channel '<channel>:<channelSerial>'. Every listening node (including the publisher) receives the notification, fetches the canonical row by (channel, channelSerial), and calls channel.Append(cm) on its local Channel — deduplicating by (channel, channelSerial) because NOTIFY is at-least-once. Send pointers (channel + serial), never payloads, to stay under NOTIFY's 8KB limit. Global ordering falls out of the serial format (the seriesId disambiguates same-millisecond serials across nodes). Single-process modes (memory/disk) bypass the broker entirely. Depends on the Postgres storage implementation.
<!-- SECTION:DESCRIPTION:END -->
