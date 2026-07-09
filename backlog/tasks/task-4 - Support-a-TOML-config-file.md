---
id: TASK-4
title: Support a TOML config file
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 16:05'
updated_date: '2026-07-09 11:40'
labels:
  - config
dependencies: []
ordinal: 4000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Add support for loading configuration from a TOML file (e.g. --config ably-server.toml). Today config is flags + env only, and DESIGN.md §9 explicitly states "No config file" with precedence flag > env > defaults. This task supersedes that: new precedence flag > env > config file > defaults. The file should cover the same keys as the §9 CLI flags (mode, listen, tls-cert/key, api-key(s), data-dir, db-dsn, message-ttl, max-messages-per-channel, shutdown-grace, log-level, log-format). Wire into the flag/env resolution in cmd/ably-server/main.go (and an internal/config package per the DESIGN layout). Update DESIGN.md §9 to document the file and the revised precedence.
<!-- SECTION:DESCRIPTION:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Add --config <path> flag for an optional TOML file covering the flags that exist today: mode, listen, api-key, data-dir, db-dsn, shutdown-grace, log-level, log-format, debug-listen (per the user's explicit instruction, narrower than the backlog description's TLS/message-ttl/max-messages-per-channel list, since those flags don't exist yet). Use github.com/BurntSushi/toml. Precedence: flag > env > config file > defaults. Put resolution in a new internal/config package (Config struct + Load(path) and a Resolve-style helper), keeping main.go's flag definitions but sourcing each flag's *default* from config-file-then-env-then-hardcoded-default so flag.Parse's normal explicit-flag-wins behaviour gives the right precedence without extra bookkeeping. Add unit tests for internal/config (TOML parsing, unknown keys/parse errors) and for main.go precedence (config file present, some flags overridden by env, some by explicit flags). Update DESIGN.md §9 to list the file's keys and confirm the precedence order (already partially documented).
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
New internal/config package (Load, PathFromArgs, Default, DefaultDuration) using github.com/BurntSushi/toml. --config resolved by hand-scanning argv (flag>env) before the rest of the flags are defined, since their defaults are seeded from the file. Added missing ABLY_SERVER_MODE/LISTEN/DATA_DIR/SHUTDOWN_GRACE/LOG_LEVEL env vars (previously undocumented gaps vs DESIGN.md's blanket 'every flag has an env equivalent' claim) so the full flag>env>file>default chain is meaningful for every key TASK-4 asked the file to cover.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Added --config <path> support for an optional TOML config file covering mode, listen, api-key, data-dir, db-dsn, shutdown-grace, log-level, log-format, debug-listen, with resolution order flag > env > config file > hardcoded default. New internal/config package (BurntSushi/toml) provides Load/PathFromArgs/Default/DefaultDuration; main.go pre-scans argv for --config (flag>env) before defining the rest of the flags, seeding each one's default from env-then-file-then-hardcoded so flag.Parse's normal explicit-flag-wins behaviour yields the full chain. Also added the previously-missing ABLY_SERVER_MODE/LISTEN/DATA_DIR/SHUTDOWN_GRACE/LOG_LEVEL env vars so every covered key actually has a working env layer. Updated DESIGN.md §9 with the full flag/env list and file precedence. Added unit tests for internal/config (TOML parsing, PathFromArgs argv forms, Default/DefaultDuration precedence) and cmd/ably-server (file-supplies-default, env-overrides-file, flag-overrides-both, malformed duration, missing file path, and an end-to-end run proving file-supplied listen+api-key actually bind). Deviation: TASK-4's backlog description also lists tls-cert/key, message-ttl and max-messages-per-channel as file keys, but per explicit instruction the file only covers the flags that exist in the codebase today (those three don't exist as flags yet, so they're out of scope).
<!-- SECTION:FINAL_SUMMARY:END -->
