# ably-server

[![CI](https://github.com/ably/ably-server/actions/workflows/ci.yml/badge.svg)](https://github.com/ably/ably-server/actions/workflows/ci.yml)

A single-binary, [Ably](https://ably.com)-compatible server. Speaks Ably's
realtime WebSocket protocol and the REST API, so existing Ably client
SDKs can connect with only a host/port override.

> **Experimental release.** Not generally available yet — the features
> listed below work, and we're looking for feedback from people using
> them before GA. See [Status](#status).

## Why this exists

Ably's cloud is the production answer for realtime messaging, but there
are situations where running a local, self-contained server is more
convenient:

- **Local development** — no internet, no shared sandbox app, no shared
  rate limits. Point your SDK at `localhost` and go.
- **CI** — a deterministic, disposable broker spun up per test run.
- **Self-hosting** — single-region deployments where the operator does
  not need (or want) Ably's cloud.

The goal is "drop-in for the use cases above": the SDK doesn't change,
only the endpoint does.

## How it works

One Go binary, three storage modes selected by `--mode`:

| Mode      | State          | Pub/sub      | Use case                          |
|-----------|----------------|--------------|-----------------------------------|
| `memory`  | in-process     | in-process   | tests, local dev, ephemeral       |
| `disk`    | embedded KV    | in-process   | single-node with persistence      |
| `cluster` | Postgres       | `LISTEN/NOTIFY` | N stateless nodes, shared DB   |

Server processes are stateless: any node can serve any connection.
There's no peer-to-peer membership or gossip — in `cluster` mode, the
database is the coordination point.

The client-visible protocol is served by
[github.com/ably/server-protocol](https://github.com/ably/server-protocol),
the same code Ably's own service runs, so what a client observes here is
not a second implementation of it. What this server supplies underneath is
the storage, the keys and the channel rules.

Supported:

- **Connections** over WebSocket, SSE and comet, so an SDK that falls back
  from WebSocket still connects.
- **Channels** — attach and detach, publish and subscribe, attachment
  continuity by `channelSerial`, and `rewind`.
- **Presence** — enter, update and leave, sync on attach, presence history.
- **History** — persisted messages, paged, on attach and over REST.
- **Mutable messages** — update, delete and append, with version history.
- **Annotations** — annotations on a message, and the summaries folded
  from them.
- **LiveObjects** — objects created and mutated over a channel, and read
  back over REST.
- **Occupancy** — served over the realtime connection.
- **Channel rules** — namespace settings, selected by name or by match
  expression.
- **Auth** — API key, Ably JWT and token requests, with Ably-style
  capabilities.
- **Reconfiguration while running** — keys, channel rules and the app's
  status are read from watched directories, so a key can be narrowed or
  revoked, a rule changed, or the app disabled, under a connected client.

Not supported: push notifications, integrations, message queues, Spaces,
Chat, LiveSync, the admin and account APIs, multi-region, statistics
(`GET /stats` is a compatibility stub), token revocation, filtered
subscriptions, and object garbage collection. See
[DESIGN.md](DESIGN.md) for why each is left out, and for the full spec.

## Quickstart

Requires [mise](https://mise.jdx.dev/) and read access to
[github.com/ably/server-protocol](https://github.com/ably/server-protocol)
over SSH — the protocol module is in a private repository for now.

```sh
mise run build-server
```

> Without mise, install Go 1.26+ yourself and set the environment from the
> `[env]` section of [`mise.toml`](mise.toml) — it resolves the private
> protocol module over SSH rather than the module proxy — then run
> `go build -o bin/ably-server ./cmd/ably-server`.

```sh
# Generate a key in the Ably format: <appId>.<keyId>:<secret>
export ABLY_API_KEY="app.key:$(openssl rand -base64 24)"
echo "$ABLY_API_KEY"

export ABLY_SERVER_KEYS="$ABLY_API_KEY"
./bin/ably-server --listen :8080
```

Publish via REST:

```sh
curl -u "$ABLY_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"name":"greeting","data":"hello"}' \
  http://localhost:8080/channels/test/messages
```

Connect with an Ably SDK by pointing it at the local host:

```go
client, _ := ably.NewRealtime(
    ably.WithKey(os.Getenv("ABLY_API_KEY")),
    ably.WithRealtimeHost("localhost"),
    ably.WithEnvironment(""),
    ably.WithPort(8080),
    ably.WithTLS(false),
)
```

### Modes

```sh
# In-memory (default)
ably-server --mode memory

# On-disk (bbolt) persistence
ably-server --mode disk --data-dir ./data

# Clustered against Postgres
export ABLY_SERVER_POSTGRES_DSN='postgres://user:pw@host:5432/db?sslmode=disable'
ably-server --mode cluster
```

Run `ably-server --help` for the full flag list. Every option can also be
set in a TOML config file passed via `--config`; see
[`config.example.toml`](./config.example.toml) for every key documented
with its default.

Most configuration is read once, at startup. Three sources are not:
`--keys-dir` and `--namespaces-dir` hold one API key and one channel rule
per file, and `--app-status-file` says whether the app is served at all
(no file means it is). Those are re-read every second, so an SDK can be
developed against a key whose capability changes, a rule that changes
under an attached channel, or an app that stops being served
mid-connection — the things a real Ably app does and a fixed
configuration cannot.

### Local cluster (Docker Compose)

To run the full `cluster` topology locally — one PostgreSQL instance and
three stateless ably-server nodes sharing it for both state and pub/sub:

```sh
docker compose up --build
```

The nodes auto-migrate the empty database on boot (under a Postgres
advisory lock), and a random API key is generated into `.cluster/` on
first run, so there's no manual setup. Each node is reachable on its own
host port and all three share that key, so a client can attach to any of
them:

| Node  | Endpoint              |
|-------|-----------------------|
| node1 | `http://localhost:8081` |
| node2 | `http://localhost:8082` |
| node3 | `http://localhost:8083` |

Publish to one node and read it back from another (the shared DB carries
the message across):

```sh
export ABLY_API_KEY=$(cat .cluster/api-key)

curl -u "$ABLY_API_KEY" -H 'Content-Type: application/json' \
  -d '{"name":"greeting","data":"hello"}' \
  http://localhost:8081/channels/test/messages

curl -u "$ABLY_API_KEY" http://localhost:8082/channels/test/history
```

The key is stable across restarts; delete `.cluster/` to rotate it.

## Benchmarking

`cmd/ably-bench` drives pub/sub load against a running server (a single
node or the Compose cluster above), checks delivery correctness, measures
end-to-end latency, and can search for the highest throughput that stays
within a latency budget. Against the Compose cluster, export its key
first — `export ABLY_SERVER_KEYS=$(cat .cluster/api-key)` — or pass
`--key`.

```sh
# Fixed-rate run against the local cluster (default endpoints):
go run ./cmd/ably-bench --rate 5000 --duration 10s

# Find the max throughput within p50<=20ms, p99<=100ms:
go run ./cmd/ably-bench --search --p50 20ms --p99 100ms --max-rate 100000

# Target a single in-memory node instead:
go run ./cmd/ably-bench --endpoints localhost:8090 --rate 20000
```

Each message carries its publisher id, a per-publisher sequence number,
and a publish timestamp; the same process publishes and subscribes, so
latency is measured against one clock with no skew. Correctness is a hard
check — any loss, duplication, or per-channel reordering fails the run.
`--search` ramps the offered load until the budget breaks, then binary-
searches for the highest sustained rate that still meets it. Run
`go run ./cmd/ably-bench --help` for the full flag list.

## Sandbox provisioner

`cmd/ably-local-sandbox` is a test-app provisioner for the Ably SDK test suites
(see [DESIGN.md §17](DESIGN.md#17-sandbox-provisioner)). `ably-server`
itself is strictly single-app, but SDK test suites expect a sandbox host
that hands out a fresh app per run; `ably-local-sandbox` bridges the gap by
spawning one isolated, in-memory `ably-server` child per provisioned app:

- `POST /apps` takes an Ably test-app-setup `post_apps` body (keys,
  namespaces, channels) and boots a child for it, returning the app JSON
  extended with `endpoint`/`port`/`tls` so a client can connect straight
  to the child.
- `DELETE /apps/{appId}` tears that child down (idempotent).

```sh
mise run build

# The sandbox finds bin/ably-server beside itself, so it needs no
# --server-bin.
./bin/ably-local-sandbox --listen :9080
```

Children are always booted with the stats stub enabled
(`--enable-stats-stub`), since compat suites POST stats fixtures before
reading them back, but the core server leaves that stub off by default.
Run `ably-local-sandbox --help` for the full flag list (idle-TTL, log
directory/level, etc).

Point an SDK test suite's sandbox host at `http://localhost:9080` to run
it against local infrastructure instead of Ably's hosted sandbox.

## Status

The surface listed under [How it works](#how-it-works) is implemented, and
checked against the ably-go, ably-js and AI Transport test suites (see
[`compat/`](compat/README.md)). What that list marks as not supported is
out of scope rather than pending — a deliberate boundary, not a roadmap.

There are not yet any stability or support guarantees, and interfaces and
behaviour may change without notice.

Feedback and bug reports are welcome via
[GitHub Issues](https://github.com/ably/ably-server/issues); see
[CONTRIBUTING.md](CONTRIBUTING.md) for how to build, test, and open a pull
request.

## Container image

Releases are published as a signed multi-architecture image built on
`scratch` — the binary and nothing else, running as a non-root user,
with a Cosign signature, a provenance attestation and an SBOM in both
SPDX and CycloneDX attached to each digest.

[docs/releases.md](docs/releases.md) covers how to verify all of that,
how to mirror the image into your own registry without losing it, and
what our patch cadence is.

Locally, `mise run image-release` builds the same image, and
`mise run sbom` and `mise run scan` produce its inventory and scan it
with the same tools CI uses. `docker compose` builds a development
variant on Alpine instead, which has a shell in it.

## Further reading

- [DESIGN.md](DESIGN.md) — full design, protocol coverage, semantics.
- [docs/releases.md](docs/releases.md) — what a release consists of and
  how to check it.
- [Ably protocol docs](https://ably.com/docs) — the protocol this
  server implements a subset of.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
