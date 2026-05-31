---
id: TASK-24
title: 'Add single-server PostgreSQL integration test (testcontainer, build-tagged)'
status: Done
assignee:
  - '@claude'
created_date: '2026-05-31 16:27'
updated_date: '2026-05-31 22:20'
labels: []
dependencies:
  - TASK-21
  - TASK-23
ordinal: 24000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Add an integration test that runs the server against a real PostgreSQL spun up via testcontainers-go, covering the Postgres storage path end to end (publish -> persist -> history / channelSerial replay). Gate it behind an explicit build tag (e.g. //go:build integration) so plain `go test ./...` runs only unit tests by default, and integration tests run via `go test -tags=integration ./...`. The test relies on auto-migrate to provision the schema. Adds the testcontainers-go dependency. Depends on the Postgres storage implementation and auto-migrate.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 A new build-tagged (//go:build integration) helper package internal/storage/postgres/pgtest provides Start(t) *Container (one-shot per test run, shared via sync.Once) and (*Container).FreshSchemaDSN(t) string. The existing postgres_test.go is updated to use it; the duplicated testcontainer setup is removed.
- [x] #2 run() in cmd/ably-server/main.go is refactored to take a runOpts struct that includes an optional Ready chan<- net.Addr field; when non-nil, run() sends the bound listener's address on it once net.Listen returns. Tests use this to discover the ephemeral port.
- [x] #3 main.go's main() and the existing main_test.go tests are updated to use the new runOpts shape; no behaviour change for production.
- [x] #4 A new cmd/ably-server/integration_test.go (//go:build integration) brings up postgres (via pgtest), starts run() with --mode=cluster --db-dsn=… --listen=127.0.0.1:0 in a goroutine, discovers the listening address via the Ready channel, runs scenarios against the live server, then cancels ctx and waits for run() to exit cleanly.
- [x] #5 Scenario A — REST publish → WS subscribe: dial a WS via the ably-go SDK, attach to a channel, publish via the REST endpoint, observe the subscribed handler receives the message. Round-trip exercises store.Store → NOTIFY → LISTEN goroutine → Appender → Channel → realtime attachment forward path.
- [x] #6 Scenario B — WS publish self-loop: same WS attaches and publishes; the publisher observes ACK plus its own MESSAGE frame, proving the unified delivery path including the local NOTIFY round-trip.
- [x] #7 Test synchronisation uses channels + a context bounded by t.Deadline()/5s as the 'never hangs' guard, not magic-number sleep timeouts; received frames are pushed onto a buffered channel and asserted via select.
- [x] #8 go build, go vet, go test ./..., go test -race ./... remain clean without the integration tag. go test -tags=integration ./cmd/ably-server/... passes against the testcontainer.
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Extract testcontainer harness from internal/storage/postgres/postgres_test.go into a new build-tagged package internal/storage/postgres/pgtest/pgtest.go. Expose Start(t) *Container and (*Container).FreshSchemaDSN(t) string. Update postgres_test.go to use it.

2. Refactor cmd/ably-server/main.go's run() to take a runOpts struct (Args, Getenv, Out, Ready chan<- net.Addr). Send the listener.Addr() on Ready once net.Listen returns. Existing main() and main_test.go updated.

3. Create cmd/ably-server/integration_test.go (//go:build integration):
   - One Postgres testcontainer via pgtest.Start at TestMain or via lazy sync.Once.
   - Per subtest: fresh schema via FreshSchemaDSN.
   - Start run() in a goroutine with --mode=cluster --db-dsn=<fresh-schema-dsn> --listen=127.0.0.1:0 --api-key=app.key:secret.
   - Block on <-ready to discover addr, derive base URL.
   - On test exit: cancel ctx, wait for run() goroutine to return.

4. Scenario A (REST → WS):
   - Build an ably-go realtime client pointed at the test server.
   - Attach to channel 'foo' (server-driven SUBSCRIBE).
   - REST publish via http.Post to /channels/foo/messages with a JSON body.
   - Subscriber receives the message via the SDK's Subscribe handler; assert within a context-bounded select.

5. Scenario B (WS self-loop):
   - One realtime client attaches to 'bar'.
   - Publishes via the SDK on the same connection.
   - Asserts the SDK's Publish call returns no error (proves ACK) AND that the same client's Subscribe handler receives the message (proves the local NOTIFY round-trip delivers).

6. Verify:
   - go build ./...
   - go test ./... (no integration tag) — passes.
   - go test -race ./... — passes.
   - go vet ./...
   - go test -tags=integration ./cmd/ably-server/... — passes against testcontainer.
   - go test -tags=integration ./... — passes (full integration suite).
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Decided to keep tests using run() rather than building a separate testable-server helper. Requires the small runOpts/Ready refactor but means the tests exercise the same code path production does — flag parsing, storage wiring, graceful shutdown.

Avoided introducing magic-number sleep timeouts: subscriber frames go to a buffered channel; tests select with a ctx bounded by t.Deadline()/5s as the only timeout guard.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Binary-level integration test for the cluster-mode publish path. Boots ably-server via run() against a real Postgres testcontainer, drives REST and WS publishes through the live HTTP server, and asserts subscribers observe the messages.

## What's in

- internal/storage/postgres/pgtest: extracted testcontainer harness as a build-tagged shared helper (Start, Container.BaseDSN, Container.FreshSchemaDSN). postgres_test.go's duplicated implementations are removed; the new cmd integration test uses it too, and TASK-25 (multi-node) will reuse it again.
- cmd/ably-server/main.go: run() now takes a runOpts struct (Args, Getenv, Out, Ready). Ready is the optional testing hook — when non-nil, run sends the listener's bound net.Addr on it once net.Listen returns. The send is ctx-bounded so a missing receiver does not deadlock.
- cmd/ably-server/main_test.go: existing tests migrated to the new runOpts shape; no behaviour change.
- cmd/ably-server/integration_test.go (//go:build integration):
  - Scenario A (TestIntegrationRESTPublishToWSSubscribe): SDK client attached to channel foo; REST POST to /channels/foo/messages; SDK Subscribe handler observes the message. Exercises REST → core.Channel.Publish → postgres.Store → NOTIFY → LISTEN goroutine → Appender → realtime forward.
  - Scenario B (TestIntegrationWSPublishSelfLoop): SDK client publishes to bar and receives its own publish back via the NOTIFY round-trip — proves the unified delivery path applies even to self-publishes.
- testCtx helper bounds the never-hangs guard by t.Deadline() (with 1s margin) or 5s if -timeout isn't set; received messages come through a buffered channel asserted via select. No magic-number sleeps.

## Verification

- go build, go vet — clean.
- go test ./... — clean (integration tests gated by tag, not run).
- go test -race ./... — clean.
- go test -tags=integration ./... — all suites pass, ~5s end to end. Both new scenarios complete in ~0.5s each against the postgres:17-alpine container.

## Coverage delta over TASK-22

TASK-22's TestPostgresClusterBrokerDeliversCrossNode proved storage→Appender works between two postgres.Storage instances. TASK-24 stitches realtime + REST + auth + run() into the same picture: real HTTP requests, real WS subscriptions via the ably-go SDK, real cluster-mode boot from --mode=cluster --db-dsn. The unified flow is now end-to-end verifiable through the actual entry point.

## What this unblocks

TASK-25 (multi-server cluster test) — same pgtest harness, two run() instances pointed at the same schema.
<!-- SECTION:FINAL_SUMMARY:END -->
