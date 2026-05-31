---
id: TASK-14
title: Support resuming an attachment via history then live tail
status: To Do
assignee: []
created_date: '2026-05-31 16:11'
labels: []
dependencies: []
ordinal: 14000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Implement channelSerial-based attachment resume per DESIGN.md §4.3/§4.4. On ATTACH carrying a channelSerial, the attachment first reads the gap from storage history (storage.History with AfterChannelSerial = the client's cursor) up to the channel's current head, forwarding those ChannelMessages, then transitions to the live linked-list tail at the resume point — with no lost or duplicated messages across the handover. If the supplied serial has aged out of retention, attach at the live head instead, clear ATTACHED.flags.RESUMED, and populate ATTACHED.error with an ErrorInfo per §4.3 (no replay). ATTACHED.channelSerial reflects the confirmed attach point. This establishes the history-then-live cursor mechanism that rewind reuses.
<!-- SECTION:DESCRIPTION:END -->
