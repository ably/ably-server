---
id: TASK-28
title: Add channel/connection labelling to segment metrics (design + implement)
status: To Do
assignee: []
created_date: '2026-05-31 17:18'
updated_date: '2026-05-31 17:18'
labels: []
dependencies:
  - TASK-27
ordinal: 28000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Give operators a way to see which channels and connections contribute the most traffic, WITHOUT exploding Prometheus cardinality — using raw channel name or clientId as a label is unbounded and unacceptable. This needs fleshing out: evaluate the approaches and pick one (or support both):

- A mapping in the config file (TASK-4) that assigns a bounded label via regex on channel name / clientId (patterns -> segment names), bucketing traffic into a small fixed label set.
- A callout to a separate process/endpoint, passed channel/connection metadata, that returns a label — allowing dynamic/external classification — with caching to bound lookup cost.

Decide the mechanism, define the label taxonomy and a default/fallback label for unmatched channels/connections, explicitly bound the resulting cardinality, and apply the label to the relevant metrics from the process-wide metrics task. Depends on the base Prometheus metrics; the config-file approach also ties into the TOML config task.
<!-- SECTION:DESCRIPTION:END -->
