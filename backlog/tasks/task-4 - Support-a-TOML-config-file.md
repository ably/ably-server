---
id: TASK-4
title: Support a TOML config file
status: To Do
assignee: []
created_date: '2026-05-31 16:05'
updated_date: '2026-06-03 13:06'
labels:
  - config
dependencies: []
ordinal: 4000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Add support for loading configuration from a TOML file (e.g. --config ably-server.toml). Today config is flags + env only, and DESIGN.md §9 explicitly states "No config file" with precedence flag > env > defaults. This task supersedes that: new precedence flag > env > config file > defaults. The file should cover the same keys as the §9 CLI flags (mode, listen, tls-cert/key, api-key(s), data-dir, db-dsn, message-ttl, max-messages-per-channel, shutdown-grace, log-level, log-format). Wire into the flag/env resolution in cmd/ably-server/main.go (and an internal/config package per the DESIGN layout). Update DESIGN.md §9 to document the file and the revised precedence.
<!-- SECTION:DESCRIPTION:END -->
