# ably-server

[![CI](https://github.com/ably/ably-server/actions/workflows/ci.yml/badge.svg)](https://github.com/ably/ably-server/actions/workflows/ci.yml)

A single-binary, [Ably](https://ably.com)-compatible server. Speaks Ably's
realtime WebSocket protocol and the core REST pub/sub endpoints, so
existing Ably client SDKs can connect with only a host/port override.

> Work in progress. This is an experimental implementation — interesting
> to look at, not yet something to depend on. See [Status](#status) below.

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

Surface area (subset of Ably's protocol — see [DESIGN.md](DESIGN.md) for
the full spec):

- **WebSocket** at `GET /` — `ATTACH` / `DETACH` / `MESSAGE` with
  `channelSerial`-based attachment continuity and `rewind`.
- **REST** — `POST/GET /channels/{name}/messages`,
  `GET /channels/{name}/presence[/history]`, `GET /time`,
  `GET /healthz`, `GET /readyz`.
- **Presence** — enter/update/leave, sync on attach, presence history.
- **Mutable messages** — message update/delete/append with version history.
- **Auth** — API key (Basic) or JWT (HS256) with Ably-style capabilities.

Out of scope: push, integrations, multi-region, Spaces, Chat,
LiveObjects, and the rest of the cloud-only product surface.

## Quickstart

Requires Go 1.26+.

```sh
# Pick any key in the Ably format: <appId>.<keyId>:<secret>
export ABLY_SERVER_KEYS=app.key:secret

go run ./cmd/ably-server --listen :8080
```

Publish via REST:

```sh
curl -u "$ABLY_SERVER_KEYS" \
  -H 'Content-Type: application/json' \
  -d '{"name":"greeting","data":"hello"}' \
  http://localhost:8080/channels/test/messages
```

Connect with an Ably SDK by pointing it at the local host:

```go
client, _ := ably.NewRealtime(
    ably.WithKey("app.key:secret"),
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

### Local cluster (Docker Compose)

To run the full `cluster` topology locally — one PostgreSQL instance and
three stateless ably-server nodes sharing it for both state and pub/sub:

```sh
docker compose up --build
```

The nodes auto-migrate the empty database on boot (under a Postgres
advisory lock), so there's no manual setup. Each node is reachable on its
own host port and all three share the key `app.key:secret`, so a
client can attach to any of them:

| Node  | Endpoint              |
|-------|-----------------------|
| node1 | `http://localhost:8081` |
| node2 | `http://localhost:8082` |
| node3 | `http://localhost:8083` |

Publish to one node and read it back from another (the shared DB carries
the message across):

```sh
curl -u app.key:secret -H 'Content-Type: application/json' \
  -d '{"name":"greeting","data":"hello"}' \
  http://localhost:8081/channels/test/messages

curl -u app.key:secret http://localhost:8082/channels/test/history
```

## Benchmarking

`cmd/ably-bench` drives pub/sub load against a running server (a single
node or the Compose cluster above), checks delivery correctness, measures
end-to-end latency, and can search for the highest throughput that stays
within a latency budget.

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
go build -o ably-server ./cmd/ably-server
go build -o ably-local-sandbox ./cmd/ably-local-sandbox

./ably-local-sandbox --listen :9080 --server-bin ./ably-server
```

Children are always booted with the stats stub enabled
(`--enable-stats-stub`), since compat suites POST stats fixtures before
reading them back, but the core server leaves that stub off by default.
Run `ably-local-sandbox --help` for the full flag list (idle-TTL, log
directory/level, etc).

Point an SDK test suite's sandbox host at `http://localhost:9080` to run
it against local infrastructure instead of Ably's hosted sandbox.

## Status

Some of [DESIGN.md](DESIGN.md) is implemented; some is still to come.
The code is not feature-complete and the protocol coverage is
partial. Treat it as a sketch you can run, not a product.

This is an experimental server whose protocol internals will
progressively be replaced by code extracted from Ably's production
realtime stack. Interfaces, wire coverage, and behaviour may change
without notice, and there are no stability or support guarantees.

Feedback and bug reports are welcome via
[GitHub Issues](https://github.com/ably/ably-server/issues); see
[CONTRIBUTING.md](CONTRIBUTING.md) for how to build, test, and open a pull
request.

## Further reading

- [DESIGN.md](DESIGN.md) — full design, protocol coverage, semantics.
- [Ably protocol docs](https://ably.com/docs) — the protocol this
  server implements a subset of.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
