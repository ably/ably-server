---
id: TASK-20
title: ACK a published message only after it is written to storage
status: To Do
assignee: []
created_date: '2026-05-31 16:11'
updated_date: '2026-05-31 16:11'
labels: []
dependencies:
  - TASK-13
ordinal: 20000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Only return ACK to the publisher once storage.AppendChannelMessage has durably persisted the ChannelMessage (NACK on failure), rather than acking optimistically. The wiring must not block the connection's read/loop goroutine while the storage write is in flight (DESIGN §5.2 — single writer, non-blocking loop): perform the append off the connection loop and deliver ACK/NACK (echoing the publish msgSerial) via the outbound writer when it completes, preserving per-connection ordering of acknowledgements. Depends on the serial-generation consolidation (which routes minting + append through storage).
<!-- SECTION:DESCRIPTION:END -->
