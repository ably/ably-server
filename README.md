# ably-server

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

Requires Go 1.25+.

```sh
# Pick any key in the Ably format: <appId>.<keyId>:<secret>
export ABLY_SERVER_API_KEY=app.key:secret

go run ./cmd/ably-server --listen :8080
```

Publish via REST:

```sh
curl -u "$ABLY_SERVER_API_KEY" \
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
export ABLY_SERVER_DB_DSN='postgres://user:pw@host:5432/db?sslmode=disable'
ably-server --mode cluster
```

Run `ably-server --help` for the full flag list.

### Local cluster (Docker Compose)

To run the full `cluster` topology locally — one PostgreSQL instance and
three stateless ably-server nodes sharing it for both state and pub/sub:

```sh
docker compose up --build
```

The nodes auto-migrate the empty database on boot (under a Postgres
advisory lock), so there's no manual setup. Each node is reachable on its
own host port and all three share the api-key `app.key:secret`, so a
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

## Status

Some of [DESIGN.md](DESIGN.md) is implemented; some is still on the
backlog. The code is not feature-complete and the protocol coverage is
partial. Treat it as a sketch you can run, not a product.

For a sense of what's planned vs. landed, see the
[`backlog/`](backlog/) directory ([Backlog.md](https://github.com/MrLesk/Backlog.md)
tasks).

## Repository layout

```
cmd/ably-server/   binary entry point
internal/auth/     API key + JWT verification, capability matching
internal/core/     channels, attachments, message fan-out
internal/protocol/ Ably ProtocolMessage codec (JSON + msgpack)
internal/realtime/ WebSocket handler
internal/rest/     REST handlers
internal/storage/  storage backends (memory, bbolt, postgres)
internal/serial/   channel serial minting
DESIGN.md          full design spec
backlog/           Backlog.md task directory
```

## Further reading

- [DESIGN.md](DESIGN.md) — full design, protocol coverage, semantics.
- [Ably protocol docs](https://ably.com/docs) — the protocol this
  server implements a subset of.
