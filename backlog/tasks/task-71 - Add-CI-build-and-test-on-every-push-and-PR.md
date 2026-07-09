---
id: TASK-71
title: 'Add CI: build and test on every push and PR'
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 11:06'
updated_date: '2026-07-09 14:09'
labels: []
dependencies: []
priority: medium
ordinal: 71000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
There is no CI, so HEAD is not guaranteed to compile and the SDK/conformance gates (TASK-67/68/69) have nothing to hang off. An experimental public release needs at least: go build, go vet, unit tests, and the build-tagged Postgres integration tests on every push/PR, as the substrate the compatibility and conformance runs plug into. PDR-090 calls for the PoC to reach 'the required level of functionality, including assurance (ie review and appropriate test coverage)'.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 GitHub Actions workflow runs build, vet, and unit tests on every push and PR
- [x] #2 Postgres integration tests (testcontainers) run in CI
- [x] #3 CI status is visible on the repo (badge or required check)
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Create .github/workflows/ci.yml with two jobs:
   - build-test: checkout@v4, setup-go@v5 (go-version-file: go.mod), go build ./..., go vet ./... , go vet -tags=integration ./..., go test -race ./... . Triggers: push to main, pull_request.
   - postgres-integration: needs docker (ubuntu-latest has it by default), checkout@v4, setup-go@v5, go test -tags=integration -race ./internal/storage/postgres/... with a timeout-minutes on the job/step.
2. Add a GitHub Actions status badge to README.md near the top (under the title/before "Work in progress" blockquote), pointing at ci.yml workflow badge URL for github.com/ably/ably-server. Note in task notes that if TASK-75 renames/moves the repo the badge URL will need updating.
3. Validate the workflow YAML locally with python yaml.safe_load, and run the equivalent go build/vet/test commands locally (including -tags=integration vet, and if Docker is available locally, the integration test invocation once).
4. Check ACs: #1 and #2 once workflows are confirmed structurally correct and locally-equivalent commands pass; #3 note the badge satisfies "visible" but a required check needs repo settings that can't be changed from here, so leave that portion unverified/noted.
5. Add --final-summary and set status Done.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Added .github/workflows/ci.yml with two jobs:
- build-test: checkout@v4, setup-go@v5 (go-version-file: go.mod), go build ./..., go vet ./..., go vet -tags=integration ./..., go test -race ./... . Triggers: push to main, pull_request.
- postgres-integration: same setup, go test -tags=integration -race ./internal/storage/postgres/... , timeout-minutes: 15. Build tag confirmed as `integration` from //go:build integration lines in postgres_test.go, presence_liveness_integration_test.go, reconnect_integration_test.go, and pgtest/pgtest.go. Uses testcontainers-go (postgres:17-alpine) which needs Docker; ubuntu-latest runners have Docker preinstalled so no extra setup step needed.

Validated locally (cannot run Actions itself):
- YAML parses cleanly (ruby -ryaml, since python3 here lacks pyyaml).
- go build ./... : OK
- go vet ./... : OK
- go vet -tags=integration ./... : OK
- go test -race ./... : all packages pass
- go test -tags=integration -race ./internal/storage/postgres/... against real Docker/testcontainers locally: all tests pass (14.2s)

Added CI badge to README.md pointing at github.com/ably/ably-server/actions/workflows/ci.yml. If TASK-75 renames or moves the repo, this badge URL will need updating to match.

AC#3 note: badge makes CI status visible on the repo page, which satisfies the "visible" wording. A required status check additionally needs a branch-protection rule in repo Settings, which is not something changeable from the CLI/repo contents here - leaving that portion as a caveat rather than blocking on it.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Added .github/workflows/ci.yml: build-test job (build, vet, vet -tags=integration, test -race) on push to main and PRs; postgres-integration job running the testcontainers-backed Postgres tests (go test -tags=integration -race ./internal/storage/postgres/..., 15 min timeout) on ubuntu-latest, which ships Docker preinstalled. Go version pinned via go-version-file: go.mod; actions pinned to checkout@v4 and setup-go@v5.

Added a CI status badge to README.md (top, under the H1) linking to the ci.yml workflow. Badge URL assumes github.com/ably/ably-server; note that TASK-75's naming decision may require updating it.

Validated locally since Actions can't run here: YAML parses (ruby -ryaml; no pyyaml available in this environment), and every step's exact command was run locally and passed - go build ./..., go vet ./..., go vet -tags=integration ./..., go test -race ./... (all packages), and go test -tags=integration -race ./internal/storage/postgres/... against a real Docker/testcontainers Postgres (all tests green, ~14s).

AC#1 and AC#2: satisfied and locally verified equivalent to what CI will run.
AC#3: satisfied via the badge (the AC's own wording is "badge or required check"); a required status check would additionally need a branch-protection rule in GitHub repo Settings, which isn't changeable from here - noted as a caveat, not blocking.
<!-- SECTION:FINAL_SUMMARY:END -->
