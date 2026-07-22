# SDK compatibility harnesses

This directory runs Ably SDK test suites against a local ably-server to gauge
how much of the wire protocol the server implements, and gates the result
against a checked-in list of known failures so CI can catch regressions.

```
compat/
  runners/
    ably-go/run.sh                    # drives an ably-go checkout's integration suite
    ably-js/run.mjs                   # drives an ably-js checkout's node suite
    ably-js/ndjson-reporter.cjs       # streaming mocha reporter (loaded by abs path)
    ably-ai-transport-js/run.mjs      # drives ably-ai-transport-js's integration specs (vitest)
  known-failures/
    ably-go.toml                      # tests currently expected to fail, with reasons
    ably-js.toml
    ably-ai-transport-js.toml
```

The gate itself is `cmd/compat-gate` (implemented in `internal/compatgate`).

## Why the runners live here, not in the SDK repos

ably-server's CI is the only thing that runs these — each SDK repo's own CI keeps
testing against the cloud sandbox and never invokes them. So the runners live
next to the `compat-gate` and known-failures lists they feed, keeping the runner,
the shared report shape, the ignore list, and the accept/reject policy all owned
in one place. A fix in ably-server can prune a stale ignore-list entry in the
same PR without touching an SDK repo.

Each SDK repo carries only the **inert `ABLY_LOCAL_SANDBOX_URL` support** in its
own test suite (it can only live there — nothing else can reach into that SDK's
fixtures and provisioning code). When the env var is unset the SDK's normal
cloud-sandbox path is unchanged. When it is set, the suite provisions each test's
app through the local sandbox's `POST /apps` and routes that test's clients at the
isolated server the sandbox booted for it.

A runner takes the SDK checkout to test as a parameter (`--sdk-dir`, defaulting to
`../ably-go` / `../ably-js` / `../ably-ai-transport-js` beside ably-server) and
drives that checkout's own test toolchain — so CI can pin each SDK to a specific
commit and check it out deterministically.

## The local sandbox is a prerequisite (treat it like a database)

The runners do **not** start a sandbox; the caller manages its lifecycle. Boot
one from this repo:

```bash
go run ./cmd/ably-local-sandbox --listen :9010
```

It must expose the Ably test-app provisioning API: `POST /apps` (returns the
app's keys plus the `endpoint`/`port`/`tls` of the isolated server it booted for
that app), `DELETE /apps/{id}`, and `POST /stats` (also the runners' liveness
probe). Point a runner at it with `--url` / `ABLY_LOCAL_SANDBOX_URL` (default
`http://localhost:9010`).

Each test gets a fresh, isolated app and its own server child, so tests never
contend over shared state and run concurrently across a worker pool. Every test
(ably-go) or test file (ably-js, ably-ai-transport-js) runs in its own process,
so a panic or crash against an unimplemented endpoint doesn't abort the batch.

## Running

```bash
# 1. boot the sandbox (leave it running)
go run ./cmd/ably-local-sandbox --listen :9010

# 2a. ably-go (checkout at ../ably-go by default)
compat/runners/ably-go/run.sh --out compat-results-ably-go.json

# 2b. ably-js (build the library in the checkout first)
(cd ../ably-js && npm run build:node && npm run build:push && npm run build:liveobjects)
node compat/runners/ably-js/run.mjs --out compat-results-ably-js.json

# 2c. ably-ai-transport-js (no build — vitest runs the TypeScript source directly;
#     just install deps in the checkout first)
(cd ../ably-ai-transport-js && pnpm install)
node compat/runners/ably-ai-transport-js/run.mjs --out compat-results-ably-ai-transport-js.json

# 3. gate each report against its known-failures list
go run ./cmd/compat-gate --results compat-results-ably-go.json --ignore compat/known-failures/ably-go.toml
go run ./cmd/compat-gate --results compat-results-ably-js.json --ignore compat/known-failures/ably-js.toml
go run ./cmd/compat-gate --results compat-results-ably-ai-transport-js.json --ignore compat/known-failures/ably-ai-transport-js.toml
```

Run any runner with `--help` for its full option list (`--sdk-dir`, `--jobs`,
`--timeout`, `--transports` for ably-js, `--run` for ably-go, ...).

## The shared report shape

All runners emit the **same JSON report object**, which `compat-gate` consumes
directly — no per-SDK reshaping:

```jsonc
{
  "generatedAt": "2026-07-21T...",
  "sdk": "ably-go",              // or "ably-js" / "ably-ai-transport-js"
  "sdkRev": "<git HEAD of the SDK checkout>",
  "localSandboxURL": "http://localhost:9010",
  "jobs": 8,
  "transports": null,           // ably-js may restrict, e.g. "web_socket"
  "complete": true,
  "tally": { "pass": 136, "fail": 13, "skip": 1 },
  "results": [
    { "name": "TestRealtimeConn_...", "verdict": "pass", "duration": 0.12 }
  ]
}
```

`compat-gate` reads only `results[]` (`name` + `verdict`); the rest is metadata
for humans reading the CI artifact. The two JS runners (ably-js,
ably-ai-transport-js) additionally carry a `files[]` per-file summary and a
`file`/`err` on each result for triage.

**Verdicts:** `pass`, `fail`, `skip` per test, plus `timeout` (killed at its cap),
`panic` (ably-go), and `error` (ably-js file produced no reporter output —
crashed or failed to start). `duration` is in seconds. A `skip` is the test's own
deliberate decision (an environment precondition it checks itself) — not a failure
to track. `compat-gate` treats `fail`/`panic`/`timeout` as failures.

## Policy-free runners, policy in the gate

The runners only **measure**: a completed run always exits 0 regardless of
individual verdicts, and exits non-zero only when the run couldn't complete (no
sandbox reachable, library not built, no matching tests). Deciding which failures
are acceptable is `compat-gate`'s job, done by diffing the report against the
known-failures list two ways:

- a failing test **not** on the list → a regression (or an untracked gap) → fail;
- a list entry that is **not** currently failing → stale (now passes, or was
  renamed/removed upstream) → fail, forcing a prune so the list can't rot.

Every ignore-list entry must carry a `reason` (a backlog task reference or a
non-goal marker) — the list is meant to be read, not just machine-diffed.

Expect some failures to be inherent to running against a plaintext local server
rather than the cloud: tests that assert cloud defaults (TLS on by default,
production hostnames, fallback-host behaviour) legitimately don't hold once
clients are pinned to the app's local endpoint. Those are honest signals to
classify in the ignore list, not runner bugs.
