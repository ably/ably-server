---
id: TASK-95
title: 'Sandbox provisioner: POST /apps boots a child ably-server per app'
status: Done
assignee:
  - '@claude'
created_date: '2026-07-12 10:39'
updated_date: '2026-07-12 11:13'
labels:
  - provisioner
  - testing
dependencies:
  - TASK-94
priority: high
ordinal: 95000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Per the design agreed with Lewis (2026-07-12): a separate provisioner process (new cmd/, working name ably-sandbox — final naming rides TASK-75) that implements the sandbox admin API by spawning one ably-server child per provisioned app, keeping the core server strictly single-app. POST /apps: accept the sandbox post_apps JSON (keys with capabilities, namespaces, channels with presence fixtures — the ably-common test-app-setup shape), generate appId/key names/secrets, write a tmp TOML config expressing all of it (the TASK-93/94 config structures), boot 'ably-server --config <tmp> --listen 127.0.0.1:0' and discover the bound port (adding a small --addr-file flag to ably-server is an acceptable clean mechanism), and respond with the sandbox-shaped app JSON (appId, keys[] with keyName/keySecret/keyStr/capability, namespaces, cipher echo) EXTENDED with endpoint/port/tls fields so harnesses can route clients at the child. DELETE /apps/{id} terminates the child. Also handle POST /stats at the provisioner (ably-js posts stats fixtures to the provisioning host): accept-and-discard 201, mirroring the server's stub. Lifecycle: an idle TTL reaper kills children not touched in N minutes; SIGTERM tears down all children; children inherit memory mode only. Document in DESIGN.md (new section — the provisioner is part of the testing/CI story, PDR-090's disposable-instance-per-test-run role).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 POST /apps with the ably-common post_apps body returns a sandbox-shaped app JSON plus endpoint/port/tls, backed by a running child with the requested keys/capabilities/namespaces/presence fixtures
- [x] #2 Clients using the returned key + endpoint pass a publish/subscribe/presence smoke test against the child
- [x] #3 DELETE /apps/{id} kills the child; idle TTL reaps leaked children; provisioner SIGTERM kills all children
- [x] #4 POST /stats at the provisioner returns 201 (accept-and-discard)
- [x] #5 Concurrent POST /apps produce isolated children (distinct ports, no shared state)
- [x] #6 DESIGN.md documents the provisioner
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add --addr-file flag (env ABLY_SERVER_ADDR_FILE) to cmd/ably-server: after net.Listen, atomically write bound addr (temp+rename) so a parent can discover the :0 port. Document in DESIGN §9.
2. New cmd/ably-sandbox: run(ctx, runOpts) mirroring ably-server's testable entry (Args/Getenv/Out/Ready). Flags --listen(:9080)/--server-bin/--idle-ttl(30m)/--log-dir/--log-level with ABLY_SANDBOX_* env.
3. POST /apps: decode post_apps (keys as maps to echo unknown fields; capability may be string or object; namespaces as maps; channels typed). Generate appId/accountId + per-key appId.keyId:secret. Emit tmp TOML via BurntSushi encoder reusing config.KeyEntry/Namespace/Channel types. Boot child ably-server --config --mode memory --listen 127.0.0.1:0 --addr-file, child in own pgid, ABLY_SERVER_* stripped from child env. Poll addr-file for port. Respond 201 sandbox JSON + endpoint/port/tls.
4. DELETE /apps/{id}: SIGTERM process group, SIGKILL after grace, idempotent 204. POST /stats: discard, 201.
5. Lifecycle: per-child wait goroutine; idle-TTL reaper; SIGTERM kills all children.
6. Server binary resolution: --server-bin else sibling of os.Executable() else 'go run ./cmd/ably-server'.
7. DESIGN.md new §15 Sandbox provisioner (renumber Testing->16, Layout->17).
8. Tests: unit (translation + response shape + escaping); e2e builds ably-server, POSTs vendored test-app-setup.json, publishes REST w/ keys[0], denied w/ subscribe-only key[3], reads seeded presence, DELETEs + verifies child dies; reaper test. go build/vet/test + -race.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implemented cmd/ably-sandbox: run() mirrors ably-server's testable entry (Args/Getenv/Out/Ready). POST /apps translates post_apps -> per-app child; DELETE /apps/{id} SIGTERMs the process group (SIGKILL after 5s grace), idempotent 204; POST /stats discards -> 201.

Binary resolution: --server-bin flag, else sibling 'ably-server' next to os.Executable(), else 'go run ./cmd/ably-server' (assumes module-root cwd). Documented in DESIGN §15.

Port discovery: added --addr-file to cmd/ably-server (env ABLY_SERVER_ADDR_FILE), writing the bound addr atomically (temp+rename) after net.Listen. Documented in DESIGN §9.

Config emission: reuse internal/config KeyEntry/Namespace/Channel/PresenceMember + BurntSushi toml encoder into a childConfig{keys,namespaces,channels} struct, so escaping round-trips through the server's own parser (unit test writes+reloads via config.Load).

Capability accepted as stringified JSON or nested object; echoed back as a JSON string; empty -> full {"*":["*"]}. Unknown key fields (e.g. revocableTokens) echoed verbatim. Child env has ABLY_SERVER_* stripped so provisioner env can't override per-app config. Children in own process group.

Log handling: each child's config + stdout/stderr go to a per-app dir under --log-dir (temp dir if unset), removed on terminate; boot-failure error includes the log tail.

Tests: unit (translate response-key shape, config round-trip incl. quote/brace escaping, capabilityString string/object forms, validation errors) + in-process e2e (builds ably-server in TestMain, POSTs vendored testdata/test-app-setup.json, publishes with keys[0], 401 with subscribe-only keys[3], reads 6 seeded presence members, DELETE + verify child dies, POST /stats 201, concurrent isolated ports, idle-TTL reaper). go build/vet/vet-integration/test all pass; go test -race ./cmd/ably-sandbox pass.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Add the sandbox provisioner (cmd/ably-sandbox) that gives Ably's SDK test harnesses a multi-app provisioning API while keeping ably-server strictly single-app.

What changed:
- New cmd/ably-sandbox command serving POST /apps, DELETE /apps/{appId}, POST /stats (default :9080, ABLY_SANDBOX_* flags/env). POST /apps translates the test-app-setup post_apps body (keys+capabilities, namespaces, channels+presence) into a temporary TOML config (emitted via the internal/config types so escaping round-trips), boots a child 'ably-server --config <tmp> --mode memory --listen 127.0.0.1:0 --addr-file <tmp>', discovers the port, and returns the sandbox app JSON (appId/accountId/keys[keyName,keySecret,keyStr,capability]/namespaces/channels/cipher) extended with endpoint/port/tls. Response preserves the harness key/namespace count invariant.
- Lifecycle: per-app child tracking with an idle-TTL reaper (default 30m), idempotent DELETE, and SIGTERM tearing down all children. Children run in their own process group (SIGTERM->SIGKILL after 5s) with ABLY_SERVER_* stripped from their env. Concurrent POST /apps yield isolated children on distinct ports.
- Binary resolution: --server-bin, else sibling of os.Executable(), else 'go run ./cmd/ably-server'.
- Added --addr-file to cmd/ably-server (atomic write of the bound address) for out-of-process port discovery.
- DESIGN.md: new §15 Sandbox provisioner (Testing->§16, Project layout->§17); documented --addr-file in §9.

Tests: unit tests for post_apps->config translation and response shape (incl. TOML escaping, capability string/object forms, validation errors); an in-process end-to-end test that builds ably-server, provisions the vendored ably-common post_apps body, publishes via REST with keys[0], is denied out-of-capability with the subscribe-only key, reads the 6 seeded presence members, deletes the app and verifies the child dies; plus POST /stats, concurrency, and reaper tests.

Verification: go build ./..., go vet ./..., go vet -tags=integration ./..., go test ./..., and go test -race ./cmd/ably-sandbox/... all pass (~6s).
<!-- SECTION:FINAL_SUMMARY:END -->
