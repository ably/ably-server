---
id: TASK-15
title: Support rewind via history then live tail
status: To Do
assignee: []
created_date: '2026-05-31 16:11'
updated_date: '2026-05-31 16:11'
labels: []
dependencies:
  - TASK-14
ordinal: 15000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Implement the rewind channel param per DESIGN.md §4.3. rewind=N (positive integer) selects an attach point N messages before the live head; rewind=<duration> (e.g. 15s, 2m) selects the attach point at the start of that window. The attachment reads the historical prefix from storage to satisfy the rewind request up to the attach point, then streams from the live tail — reusing the history-then-live cursor mechanism from the resume task. ATTACHED.channelSerial reflects the resulting attach point. rewind and channelSerial are mutually exclusive on one ATTACH; channelSerial wins if both are supplied (rewind ignored). All other channel params are silently ignored. Depends on the resume-via-history task.
<!-- SECTION:DESCRIPTION:END -->
