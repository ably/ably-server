# ably-server

[![CI](https://github.com/ably/ably-server/actions/workflows/ci.yml/badge.svg)](https://github.com/ably/ably-server/actions/workflows/ci.yml)

A single-binary, [Ably](https://ably.com)-compatible server. It speaks Ably's
realtime WebSocket protocol and its REST API, so existing Ably client SDKs can
connect to it with only a host and port override.

The client-visible protocol is served by
[github.com/ably/server-protocol](https://github.com/ably/server-protocol),
the same code that Ably's own service runs, so what a client observes here is
not a second implementation of the protocol. This server supplies the storage,
the keys and the channel rules underneath it.

> **Experimental release.** This is not generally available yet. The features
> listed below work, and we are looking for feedback from people using them
> before GA. See [Status](#status).

## Use Cases

Ably's cloud offering is a highly available, globally distributed platform for
low latency realtime messaging at scale.

ably-server is much smaller in scope. It runs as a single process, or as a small
number of processes sharing one Postgres database in a single region. That makes
it a good fit for the cases where you want Ably's protocol and client SDKs without
connecting to the cloud service:

- **Local development.** No internet connection, no shared sandbox app, and no
  shared rate limits. Point your SDK at `localhost` instead.
- **CI.** A deterministic, disposable server started and thrown away per test
  run.
- **Self-hosting.** Single-region deployments where you do not want to depend
  on Ably's cloud.

In all three cases your client code stays the same. Only the endpoint changes.

## Features

ably-server supports the core features offered by Ably cloud:

- **Connections** over WebSocket, SSE and comet, so an SDK that falls back from
  WebSocket still connects.
- **Channels**: attach and detach, publish and subscribe, attachment continuity
  by `channelSerial`, and `rewind`.
- **Presence**: enter, update and leave, sync on attach, and presence history.
- **History**: historical messages, paged, on attach and over REST.
- **Mutable messages**: update, delete and append messages, with version history.
- **Annotations**: annotations on a message, and the summaries folded from
  them.
- **LiveObjects**: objects created and mutated over a channel, and read back
  over REST.
- **Occupancy**, served as meta messages over realtime connections.
- **Channel rules**: channel specific settings, selected by name or by match
  expression.
- **Auth**: API keys, Ably JWTs and token requests, with Ably-style
  capabilities.
- **Reconfiguration while running**: add, remove, or revoke keys, update channel rules,
  or completely disable all traffic without restarting the server.

The following Ably features are not currently supported:
- Push notifications
- Integrations
- Queues
- Chat
- LiveSync
- Control API
- Stats
- Token revocation

## Getting Started

### Build

The build process uses [mise](https://mise.jdx.dev/), and requires read access to
[github.com/ably/server-protocol](https://github.com/ably/server-protocol)
over SSH, because the protocol module is in a private repository for now.

```sh
mise run build-server
```

If you don't want to install mise, install Go 1.26 or later, set the environment
from the `[env]` section of [`mise.toml`](mise.toml), which resolves the private
protocol module over SSH rather than through the module proxy, then build directly:

```sh
go build -o bin/ably-server ./cmd/ably-server
```

### Start the server

ably-server supports three storage modes, selected by `--mode`:

| Mode      | State       | Pub/sub         | Use case                      |
|-----------|-------------|-----------------|-------------------------------|
| `memory`  | in-process  | in-process      | tests, local dev, ephemeral   |
| `disk`    | embedded KV | in-process      | single node with persistence  |
| `cluster` | Postgres    | `LISTEN/NOTIFY` | N stateless nodes, shared DB  |

Server processes are stateless, so any process can serve any connection. There is
no peer-to-peer membership or communication between processes. In `cluster` mode the
database is the only coordination point.

#### memory

The default mode. All state lives in the process and is lost when it stops.
Generate an API key in the Ably format, `<appId>.<keyId>:<secret>`, and pass it
to the server:

```sh
export ABLY_API_KEY="app.key:$(openssl rand -base64 24)"

./bin/ably-server --mode memory --listen :8080 --keys "$ABLY_API_KEY"
```

#### disk

State is persisted to an embedded [bbolt](https://github.com/etcd-io/bbolt)
database under `--data-dir`, so messages, presence and objects survive a restart:

```sh
export ABLY_API_KEY="app.key:$(openssl rand -base64 24)"

./bin/ably-server --mode disk --data-dir ./data --listen :8080 \
  --keys "$ABLY_API_KEY"
```

#### cluster

Several stateless processes share one PostgreSQL database, for state and for
pub/sub between them. Point a process at your database with `--postgres-dsn`:

```sh
export ABLY_API_KEY="app.key:$(openssl rand -base64 24)"

./bin/ably-server --mode cluster --listen :8080 --keys "$ABLY_API_KEY" \
  --postgres-dsn 'postgres://user:pw@host:5432/db?sslmode=disable'
```

The schema is created on first boot, under a Postgres advisory lock so that
several processes can start at once against an empty database. To add capacity,
start more processes with the same DSN and the same keys. They share all state,
so a client can connect to any of them.

To run that topology locally, one Postgres instance and three ably-server
processes, use Docker Compose:

```sh
docker compose up --build
```

The stack needs no manual setup, because an init container generates a random
API key into `.cluster/` before the processes start. Each one is published on
its own host port and all three share that key:

| Node  | Endpoint                |
|-------|-------------------------|
| node1 | `http://localhost:8081` |
| node2 | `http://localhost:8082` |
| node3 | `http://localhost:8083` |

Read the generated key back with:

```sh
export ABLY_API_KEY=$(cat .cluster/api-key)
```

The key is stable across restarts. Delete `.cluster/` to rotate it.

### Publish over REST

With a server running and `ABLY_API_KEY` set, publish a message with `curl`:

```sh
curl -u "$ABLY_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"name":"greeting","data":"hello"}' \
  http://localhost:8080/channels/test/messages
```

Read it back from the channel's history:

```sh
curl -u "$ABLY_API_KEY" http://localhost:8080/channels/test/history
```

In `cluster` mode, publishing to one node and reading from another works
because the shared database carries the message across:

```sh
curl -u "$ABLY_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"name":"greeting","data":"hello"}' \
  http://localhost:8081/channels/test/messages

curl -u "$ABLY_API_KEY" http://localhost:8082/channels/test/history
```

### Connect with an Ably SDK

Any Ably client SDK works. Override the host and port, and turn off TLS if the
server is not behind a TLS terminator. In Go:

```go
client, _ := ably.NewRealtime(
    ably.WithKey(os.Getenv("ABLY_API_KEY")),
    ably.WithRealtimeHost("localhost"),
    ably.WithEnvironment(""),
    ably.WithPort(8080),
    ably.WithTLS(false),
)
```

### Configuration

Run `ably-server --help` for the full flag list. Every option can also be set
in a TOML config file passed via `--config`, or in an environment variable. See
[`config.example.toml`](./config.example.toml), which documents every key with
its default.

Most configuration is read once, at startup. Three sources are not.
`--keys-dir` and `--namespaces-dir` hold one API key and one channel rule per
file, and `--app-status-file` says whether the app is served at all, with no
file meaning that it is. Those three are re-read every second, and a namespace
file's modification time is what marks it as edited. That lets you develop an
SDK against a key whose capability changes, a rule that changes under an
attached channel, or an app that stops being served mid-connection, which are
things a real Ably app does and a fixed configuration cannot.

## Benchmarking

`cmd/ably-bench` drives pub/sub load against a running server, either a single
node or the Compose cluster above. It checks delivery correctness, measures
end-to-end latency, and can search for the highest throughput that stays within
a latency budget. Against the Compose cluster, export its key first with
`export ABLY_SERVER_KEYS=$(cat .cluster/api-key)`, or pass `--key`.

```sh
# Fixed-rate run against the local cluster (default endpoints):
go run ./cmd/ably-bench --rate 5000 --duration 10s

# Find the max throughput within p50<=20ms, p99<=100ms:
go run ./cmd/ably-bench --search --p50 20ms --p99 100ms --max-rate 100000

# Target a single in-memory node instead:
go run ./cmd/ably-bench --endpoints localhost:8090 --rate 20000
```

Each message carries its publisher id, a per-publisher sequence number, and a
publish timestamp. The same process publishes and subscribes, so latency is
measured against one clock with no skew. Correctness is a hard check: any loss,
duplication, or per-channel reordering fails the run. `--search` ramps the
offered load until the budget breaks, then binary-searches for the highest
sustained rate that still meets it. Run `go run ./cmd/ably-bench --help` for
the full flag list.

## Sandbox provisioner

`cmd/ably-local-sandbox` is a test-app provisioner for the Ably SDK test suites
(see [DESIGN.md §17](DESIGN.md#17-sandbox-provisioner)). `ably-server` itself
serves a single app, but SDK test suites expect a sandbox host that hands out a
fresh app per run. `ably-local-sandbox` bridges that gap by spawning one
isolated, in-memory `ably-server` child per provisioned app:

- `POST /apps` takes an Ably test-app-setup `post_apps` body (keys, namespaces,
  channels) and boots a child for it, returning the app JSON extended with
  `endpoint`, `port` and `tls` so a client can connect straight to the child.
- `DELETE /apps/{appId}` tears that child down, and is idempotent.

```sh
mise run build

# The sandbox finds bin/ably-server beside itself, so it needs no
# --server-bin.
./bin/ably-local-sandbox --listen :9080
```

Children are always booted with the stats stub enabled
(`--enable-stats-stub`), because compat suites POST stats fixtures before
reading them back, but the core server leaves that stub off by default. Run
`ably-local-sandbox --help` for the full flag list, covering idle TTL, log
directory and level, and so on.

Point an SDK test suite's sandbox host at `http://localhost:9080` to run it
against local infrastructure instead of Ably's hosted sandbox.

## Status

Everything listed under [Features](#features) is implemented, and checked
against the ably-go, ably-js and AI Transport test suites (see
[`compat/`](compat/README.md)).

There are not yet any stability or support guarantees, and interfaces and
behaviour may change without notice.

Feedback and bug reports are welcome via
[GitHub Issues](https://github.com/ably/ably-server/issues). See
[CONTRIBUTING.md](CONTRIBUTING.md) for how to build, test, and open a pull
request.

## Container image

Releases are published as a signed multi-architecture image built on
`scratch`: the binary and nothing else, running as a non-root user, with a
Cosign signature, a provenance attestation, and an SBOM in both SPDX and
CycloneDX attached to each digest.

[docs/releases.md](docs/releases.md) covers how to verify all of that, how to
mirror the image into your own registry without losing it, and what our patch
cadence is.

Locally, `mise run image-release` builds the same image, and `mise run sbom`
and `mise run scan` produce its inventory and scan it with the same tools CI
uses. `docker compose` builds a development variant on Alpine instead, which
has a shell in it.

## Further reading

- [DESIGN.md](DESIGN.md): the full design, protocol coverage and semantics.
- [docs/releases.md](docs/releases.md): what a release consists of and how to
  check it.
- [Ably protocol docs](https://ably.com/docs): the protocol that this server
  implements a subset of.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
