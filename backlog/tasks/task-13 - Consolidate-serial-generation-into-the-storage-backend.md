---
id: TASK-13
title: Consolidate serial generation into the storage backend
status: To Do
assignee: []
created_date: '2026-05-31 16:11'
labels: []
dependencies: []
ordinal: 13000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Serial minting is currently duplicated: core.Channel.Append mints a channelSerial via its own serial.Generator and stamps each Message.Serial, while internal/storage/storage.go documents that the backend owns minting and persists monotonic state (DESIGN §6/§8). Make storage the sole authority: AppendChannelMessage mints the channelSerial, stamps each Message.Serial = "<channelSerial>:<idx>", persists, and returns the resulting ChannelMessage. Remove the gen field and the minting from core.Channel; change the publish path so Channel.Append takes the already-minted ChannelMessage (channel.Append(cm)) rather than raw messages. This lets persistent backends restore generator monotonicity across restarts (the bbolt _meta bucket). Update the DESIGN §5.1 Channel sketch to match. Foundational for "always expose a channelSerial" and "ACK only after storage write".
<!-- SECTION:DESCRIPTION:END -->
