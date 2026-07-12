---
id: TASK-95
title: 'Sandbox provisioner: POST /apps boots a child ably-server per app'
status: To Do
assignee: []
created_date: '2026-07-12 10:39'
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
- [ ] #1 POST /apps with the ably-common post_apps body returns a sandbox-shaped app JSON plus endpoint/port/tls, backed by a running child with the requested keys/capabilities/namespaces/presence fixtures
- [ ] #2 Clients using the returned key + endpoint pass a publish/subscribe/presence smoke test against the child
- [ ] #3 DELETE /apps/{id} kills the child; idle TTL reaps leaked children; provisioner SIGTERM kills all children
- [ ] #4 POST /stats at the provisioner returns 201 (accept-and-discard)
- [ ] #5 Concurrent POST /apps produce isolated children (distinct ports, no shared state)
- [ ] #6 DESIGN.md documents the provisioner
<!-- AC:END -->
