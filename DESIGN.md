# ably-server — Design

## 1. Goals & non-goals

### Goals

- A single Go binary, `ably-server`, that speaks Ably's realtime WebSocket
  protocol and the core REST pub/sub endpoints.
- Drop-in for **local development** and **CI** — existing Ably client SDKs
  should connect to it without code changes (only host/port/TLS overrides).
- **Self-hostable** for single-region deployments where the operator does not
  need (or want) Ably's cloud.
- Three deployment modes selected by configuration:
  1. `memory` — single process, in-memory state, in-memory pub/sub.
  2. `disk` — single process, on-disk persistence, in-memory pub/sub.
  3. `cluster` — N processes, shared database for both state and pub/sub.
- Stateless server processes: any node can serve any connection; no peer-to-peer
  membership or gossip.

### Non-goals

- Multi-region / global distribution.
- Presence (members on a channel, presence enter/update/leave, presence
  sync, presence history) — out of scope for this design; will be
  considered separately.
- Ably-cloud-only product surface: integrations / rules, push notifications,
  Spaces, Chat, LiveObjects/LiveSync, message queues, account/app management
  APIs, statistics endpoints, the `/keys` admin API.
- Hard durability or HA guarantees beyond what the chosen database provides.
- Backwards compatibility with arbitrary historical Ably protocol versions —
  we target v2 and later.

## 2. External surface

### 2.1 Realtime (WebSocket)

`GET /` upgrades to WebSocket. The upgrade request carries:

- **Authentication** — via `Authorization` header or `key` / `accessToken`
  query parameter (see §3).
- **Protocol version** — required `v=N` query parameter; the server accepts
  `v=2` and above and rejects older versions with an `ERROR` frame and
  close.
- **Format** — `format` query parameter selects `json` (text frames,
  default) or `msgpack` (binary frames).

Each frame is a single `ProtocolMessage`. The server emits `CONNECTED` as
the first frame after a successful upgrade.

Supported `Action` values:

| Action | In | Out | Notes |
|---|---|---|---|
| `HEARTBEAT` (0) | ✓ | ✓ | server-driven keepalive + client pings |
| `ACK` (1) | | ✓ | publish acknowledgement for `msgSerial` |
| `NACK` (2) | | ✓ | publish rejection for `msgSerial` |
| `CONNECTED` (4) | | ✓ | first frame after upgrade; carries `connectionId` |
| `DISCONNECT` (5) | ✓ | | client signalling intent to disconnect |
| `DISCONNECTED` (6) | | ✓ | server-initiated disconnect with reason |
| `CLOSE` (7) | ✓ | | client requests a clean connection close |
| `CLOSED` (8) | | ✓ | server confirms close |
| `ERROR` (9) | | ✓ | |
| `ATTACH` (10) | ✓ | | client requests channel attach (see §4) |
| `ATTACHED` (11) | | ✓ | attach ack (see §4) |
| `DETACH` (12) | ✓ | | client requests channel detach |
| `DETACHED` (13) | | ✓ | detach ack |
| `MESSAGE` (15) | ✓ | ✓ | publish + delivery |

### 2.2 REST

All REST endpoints live under the root and accept either `application/json` or
`application/x-msgpack` for both request and response bodies (driven by
`Accept` / `Content-Type`).

| Method | Path | Purpose |
|---|---|---|
| POST | `/channels/{channel}/messages` | publish 1..N messages |
| GET | `/channels/{channel}/messages` | history (paginated) |
| GET | `/time` | server time (ms since epoch) |
| GET | `/healthz` | liveness — no auth |
| GET | `/readyz` | readiness — DB ping in `cluster` mode |

Pagination follows Ably's `Link` header convention (`first`, `next`).

## 3. Authentication & authorisation

A single API key is configured via `ABLY_SERVER_API_KEY` in the canonical
Ably format `appId.keyId:keySecret` (so SDKs that parse the key work
unchanged). Tokens are minted by the SDK or out-of-band — the server only
**verifies** them, it does not issue them.

Two accepted credential forms:

1. **Basic auth** — `Authorization: Basic base64(appId.keyId:keySecret)` or
   `?key=...`. Carries the API key's full capability.
2. **JWT** — bearer token signed with `keySecret` (HS256). Passed via
   `Authorization: Bearer <jwt>` or `?accessToken=...`.

JWT claims:

| Claim | Required | Purpose |
|---|---|---|
| `iat` | ✓ | issued-at; server rejects clock-skewed tokens beyond a small leeway |
| `exp` | ✓ | expiry |
| `x-ably-capability` | | JSON object granting per-channel ops (see §3.1). Absent → token inherits the signing key's capability, which for our single-key model is `{"*":["*"]}` (i.e. permissive by default; the claim is only needed to *narrow* access) |
| `x-ably-clientId` | | string; controls the connection's `clientId` (see §3.2) |

### 3.1 Capabilities

`x-ably-capability` is a JSON object of the form
`{ "<resource>": ["<op>", ...] }` where:

- `<resource>` is a channel name pattern. The server uses Ably's standard
  wildcard semantics ([docs](https://ably.com/docs/auth/capabilities#wildcards)):
  wildcards replace whole `:`-delimited segments, a `*` at the **end** of
  the pattern can match any number of trailing segments, and elsewhere
  matches exactly one. So `*` matches every channel, `foo:*` matches
  `foo:bar` and `foo:bar:baz`, and `foo:*:baz` matches `foo:bar:baz` but
  not `foo:bar:bam:baz`. `foo*` (no `:` before the `*`) is a literal
  channel name. Character classes (`[a-z]`) and `**` are not supported.
  The `[queue]*` / `[meta]*` resource prefixes do not apply since neither
  queues nor metachannels are in scope.
- `<op>` is one of `publish`, `subscribe`, `history`. `*` matches any op.

Every authenticated request resolves to a **capability set**. For each
operation the server computes the union of granted ops across all matching
resources and checks that the requested op is in the union. If not, the
operation is rejected:

| Surface | Op required |
|---|---|
| WS `ATTACH` flag `SUBSCRIBE` | `subscribe` |
| WS `ATTACH` flag `PUBLISH` | `publish` |
| WS inbound `MESSAGE` | `publish` (and the attachment must hold the `PUBLISH` mode flag, granted at attach time) |
| REST `POST .../messages` | `publish` |
| REST `GET .../messages` | `history` |

`ATTACH` mode resolution: the effective mode set delivered in `ATTACHED.flags`
is `requested ∩ capability-permitted`. Empty intersection → `ERROR` with
`code: 40160` (insufficient capabilities) and the channel is not attached.

### 3.2 Client ID

The connection's `clientId` is stamped by the server onto every outbound
`Message.clientId` published by that connection. The client cannot
override it: inbound `Message` frames whose `clientId` is set to anything
other than the connection's resolved value are rejected with `NACK`.

Resolution depends on the credential and the `clientId` query parameter on
the upgrade (WS) or request (REST):

| Credential | `x-ably-clientId` claim | `clientId` query param | Resolved `clientId` |
|---|---|---|---|
| Basic | n/a | absent | none (anonymous) |
| Basic | n/a | `<value>` | `<value>` |
| JWT | absent | absent | none (anonymous) |
| JWT | absent | `<value>` | rejected (no permission to assert clientId) |
| JWT | `<concrete>` | absent | `<concrete>` (claim value) |
| JWT | `<concrete>` | `<concrete>` matching claim | `<concrete>` |
| JWT | `<concrete>` | `<value>` ≠ claim | rejected |
| JWT | `*` | absent | none (anonymous) |
| JWT | `*` | `<value>` | `<value>` |

`*` is a wildcard marker that means "the bearer of this token may assume any
`clientId`". It is **never** itself used as a `clientId` on messages — if
the bearer wants to be identified, they must select a concrete value via
the `clientId` query parameter.

A connection or REST request that fails the table above is rejected at
auth time (WS: `ERROR` then close; REST: `401`).

## 4. Attachments

An **attachment** is the relationship between one connection and one channel.
It is created by an inbound `ATTACH` and torn down by `DETACH`, by the
connection closing, or by an unrecoverable error.

### 4.1 Lifecycle

```
client                        server
  │ ── ATTACH(channel, flags, params, channelSerial?) ──▶
  │                              │ resolve cap ∩ flags  → effective modes
  │                              │ open/find Channel
  │                              │ choose attach point
  │   ◀───── ATTACHED(flags, channelSerial) ────│
  │   ◀───── MESSAGE × N (replay, if any) ──────│
  │   ◀───── MESSAGE … (live) ───────────────────│
  │
  │ ── DETACH ──▶
  │                              │ remove from Channel
  │   ◀──── DETACHED ────────────│
```

`ATTACHED` is the **first** frame the server emits in response to `ATTACH`;
any replay or live messages follow it. Its fields:

- `flags` — the **effective** mode set (see §4.2).
- `channelSerial` — the **confirmed attach point**: the serial *from which*
  the message stream the client is now subscribed to begins. Subsequent
  `MESSAGE` frames carry their own serials advancing from that point, and
  the SDK uses the most recent serial it has seen as its cursor for any
  future re-`ATTACH`.

### 4.2 Modes

The `ATTACH.flags` bitfield selects the subset of `SUBSCRIBE`, `PUBLISH`
the client wants on this attachment. If `flags` is absent or zero the
server treats it as the full set (matches SDK default).

The effective mode set is `requested ∩ capability-permitted`, where the
permitted set is derived from the per-op capability mapping in §3:

- `SUBSCRIBE` permitted iff cap grants `subscribe` on the channel.
- `PUBLISH` permitted iff cap grants `publish`.

Empty intersection → the attach is rejected with `ERROR` (`code: 40160`)
and no channel state is created. Otherwise `ATTACHED.flags` carries the
effective set.

Once attached, modes gate frame flow:

- An attachment without `SUBSCRIBE` does not receive `MESSAGE` frames.
- Inbound `MESSAGE` from an attachment without `PUBLISH` is rejected with
  `NACK`.

### 4.3 Replay (`channelSerial` and `rewind`)

The v2 protocol holds **no per-connection server state across disconnects**.
There is no recovery TTL, no retained outbox, no resume buffer. Connection-
level `recover` / `resume` parameters are accepted on the upgrade URL for
SDK compatibility but are no-ops.

Continuity instead lives at the attachment level, driven by the client:

- The SDK tracks `channelSerial` from `ATTACHED` and every inbound
  `MESSAGE` for each attachment.
- On a fresh `ATTACH` (after reconnect, or detach/attach) it supplies the
  last-seen `channelSerial`.
- The server picks an attach point at-or-before that serial, returns it in
  `ATTACHED.channelSerial`, and then streams the messages from that point
  onwards (the gap between the client's cursor and the live head, followed
  by live traffic).
- If the supplied serial is older than retained history (the relevant
  messages have aged out — message TTL defaults to 2 minutes, see §6) the
  server still attaches: it picks the channel's current head as the attach
  point, clears `ATTACHED.flags.RESUMED`, and populates `ATTACHED.error`
  with an `ErrorInfo` explaining that the requested resume could not be
  satisfied so the SDK can surface a discontinuity to the application. No
  replay is delivered in this case.

`rewind` is the only honoured channel param. `rewind=N` (positive integer)
selects an attach point N messages before the live head; `rewind=<duration>`
(e.g. `15s`, `2m`) selects the attach point at the start of that duration.
The `ATTACHED.channelSerial` reflects the resulting attach point and the
historical messages then stream as ordinary `MESSAGE` frames. All other
channel params are silently ignored.

`rewind` and `channelSerial` are mutually exclusive on a single `ATTACH`:
if both are supplied, `channelSerial` wins (it is the more precise cursor)
and `rewind` is ignored.

### 4.4 Implementation

An attachment is a per-node (connection, channel) pair, structured as a
cursor over the Channel's linked list of entries (§5.1).

```go
type Attachment struct {
    conn    *Connection
    channel *Channel
    flags   ChannelMode
    serial  string

    e *entry // current cursor
}
```

A single goroutine per attachment runs the cursor loop:

```go
for {
    select {
    case <-a.e.notify:
        a.e = a.e.next
        a.forward(a.e.msg) // gated by mode flags
    case <-a.ctx.Done():
        return
    }
}
```

`forward` writes a `MESSAGE` `ProtocolMessage` onto the connection's
outbound queue; the connection's single writer goroutine (§5.2)
serialises actual frame writes. There is no per-attachment buffered
fan-out channel: each attachment proceeds at its own pace.

The starting cursor depends on how the attachment was created:

- Fresh attach (no `channelSerial`, no `rewind`) — `a.e = channel.Tail()`,
  so the first iteration parks on `notify` and wakes on the next live
  publish.
- Resume by `channelSerial` — the attachment first reads the gap from
  history storage, walking those messages directly (without going through
  the linked list), then transitions to the live list at the resume
  point.
- `rewind=N` / `rewind=<duration>` — same pattern: read the historical
  prefix from storage, then attach to the live tail.

Backpressure on a slow attachment is not a buffer overflow event; it
manifests as the cursor lagging the live tail. If the lag exceeds the
configured threshold (entries-behind, age-behind, or both) the attachment
is closed with `ERROR` (`code: 50000`).

## 5. Internal architecture

```
                    ┌─────────────────────────────────────┐
                    │              ably-server             │
                    │                                      │
   client ── WS ───▶│  realtime/  ── ConnectionLoop ──┐   │
                    │      │                          │   │
   client ── HTTP ─▶│   rest/   ── Handlers ──────────┤   │
                    │      │                          │   │
                    │      ▼                          ▼   │
                    │   ┌─────────────────────────────────┴──┐
                    │   │       core/Channel manager          │
                    │   │  - Channel: linked list of entries  │
                    │   │  - Attachments: cursors on the list │
                    │   └─────────────────────────────────────┘
                    │            │                ▲           │
                    │            ▼                │ NOTIFY    │
                    │      Storage iface  ────────┘ (cluster) │
                    │            │                            │
                    └────────────┼────────────────────────────┘
                                 ▼
                       memory / disk / database
```

Protocol types (`ProtocolMessage`, `Action`, `Message`) default to
importing `github.com/ably/ably-go/ably/proto` where the exported types
have the fields we need. If we hit friction — missing fields, awkward
serialisation, types not exported — the package falls back to internal
definitions in `internal/protocol/`. The fallback is mechanical:
redefine the affected struct, keep field tags, leave the rest of the
package on the upstream types.

Major packages (proposed):

```
cmd/ably-server/        # main, flag/env wiring
internal/config/        # mode + DSN resolution
internal/protocol/      # codec wrappers (json/msgpack); fallback type defs if needed
internal/auth/          # key parsing, basic-auth, JWT verify, capability + clientId resolution
internal/realtime/      # WebSocket upgrade, ConnectionLoop, attachment cursor loop
internal/rest/          # HTTP handlers + router
internal/core/          # Channel, ChannelManager, entry list, message semantics
internal/storage/       # Storage interface + memory/disk/db backends
internal/cluster/       # Postgres LISTEN/NOTIFY broker (cluster mode only)
internal/id/            # connection IDs, message IDs, msgSerial helpers
```

### 5.1 Channel

A `Channel` is a per-node, per-name structure holding the live message
list. It has **no goroutine of its own**: concurrency is serialised by a
mutex around append. Attachments tail the list at their own pace, with no
fan-out channels and no per-attachment buffering.

```go
type entry struct {
    msg    *Message
    notify chan struct{} // closed once next is set
    next   *entry
}

type Channel struct {
    name string

    mu   sync.Mutex
    tail *entry           // most recent entry
}

func (c *Channel) Append(msg *Message) {
    c.mu.Lock()
    defer c.mu.Unlock()
    e := &entry{msg: msg, notify: make(chan struct{})}
    if c.tail != nil {
        c.tail.next = e
        close(c.tail.notify) // wake all attachments parked on the prior tail
    }
    c.tail = e
}

func (c *Channel) Tail() *entry { /* mu-protected read */ }
```

The first `ATTACH` to a name (or the first publish) creates the Channel.
The Channel is removed from the manager only when **both** are true:

- it has no attachments, and
- every entry on its linked list has aged past the message TTL (default
  2 minutes; see §6).

Holding the Channel for the TTL window after the last detach keeps the
in-process list available to serve a fresh `ATTACH` that arrives soon
after with a `channelSerial` covering still-live messages, without having
to re-materialise the list from storage.

**Memory.** Go's GC reclaims entries once no attachment retains a
reference. A slow attachment retains the prefix of the list between its
cursor and the live tail, so memory grows with its lag. The retention
policy (§6) bounds the working set: once a message ages past the TTL or
the per-channel `max_messages` cap, the Channel drops its own
back-pointer to it, so any unreferenced entries become eligible for GC.
A persistently slow attachment that holds onto stale entries will be
disconnected once its lag exceeds a configurable threshold (`ERROR`
`code: 50000`).

### 5.2 Connection loop

Each WebSocket connection has:

- A read goroutine that decodes inbound frames and dispatches on `Action`.
- A write goroutine that serializes outbound frames (only one writer per
  connection per gorilla/websocket conventions).
- A heartbeat ticker that sends `HEARTBEAT` if idle.
- An `attachments map[string]*Attachment` keyed by channel name.

Inbound `MESSAGE` is routed to the matching attachment, which
authorises the publish, persists via `Storage.AppendMessages` (or, in
cluster mode, lets the Postgres `INSERT` + `NOTIFY` round-trip do the
local append), and replies with `ACK` / `NACK`. Inbound `ATTACH` /
`DETACH` are handled by the connection itself, calling into
`ChannelManager` to get/release a Channel.

## 6. Storage

```go
type Storage interface {
    Channel(name string) ChannelStore
    Close() error
}

type ChannelStore interface {
    AppendMessages(ctx context.Context, msgs []core.Message) error
    History(ctx context.Context, q HistoryQuery) (HistoryPage, error)
}
```

### 6.1 Memory backend

Per-channel ring buffer for messages, bounded by count *and* age.

### 6.2 Disk backend

SQLite via `modernc.org/sqlite` (pure-Go, no CGO). Single file at
`--data-dir/ably.db`, WAL mode. The schema is the same as the cluster
backend modulo pub/sub, so the SQL layer is shared.

### 6.3 Database backend (cluster mode)

Postgres only. LISTEN/NOTIFY gives us pub/sub and the same connection
serves as the durable store, so cluster mode needs nothing beyond a
single Postgres.

Schema sketch:

```sql
CREATE TABLE channels (
  name TEXT PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE messages (
  channel       TEXT        NOT NULL,
  msg_serial    BIGINT      NOT NULL,        -- monotonic per-channel
  id            TEXT        NOT NULL,        -- "{connId}:{msgSerial}:{idx}"
  client_id     TEXT,
  conn_id       TEXT,
  name          TEXT,
  data          BYTEA,
  encoding      TEXT,
  extras        JSONB,
  timestamp     TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (channel, msg_serial)
);
CREATE INDEX ON messages (channel, timestamp DESC);
```

Retention is enforced by a periodic background job (delete by `timestamp <
now() - ttl`) plus a per-channel `max_messages` cap.

The default message TTL is **2 minutes**, matching Ably cloud's default;
both the TTL and the per-channel message cap are configurable (see §9).
Operators who want full message history for a self-host deployment can
set the TTL to a large value.

## 7. Pub/Sub

Pub/sub is the mechanism that turns a *publish* (originating from any
node, via WS or REST) into appended entries on the **local** Channel's
linked list (§5.1) on every node that has attachments to that channel.
There is no per-attachment fan-out channel; attachments tail the list
directly.

### 7.1 Single-process modes (`memory`, `disk`)

There is no pub/sub abstraction. A publish is a direct sequence:

1. Authorise.
2. `Storage.AppendMessages` — assigns the canonical `msg_serial`.
3. `channel.Append(msg)` on the in-process Channel.

All attachments are on the same node and tail the same list.

### 7.2 Cluster mode (Postgres broker)

Each node runs a single goroutine on a dedicated Postgres connection
listening for channel-publish notifications:

- The publishing node `INSERT`s the row into `messages` — this assigns
  the global `msg_serial` — and emits
  `NOTIFY ably_channel, '<channel-name>:<row-id>'`.
- Every listening node (including the publisher) receives the notification,
  fetches the row by id, and calls `channel.Append(msg)` on its local
  Channel.

Routing the publisher's own message back through NOTIFY keeps a single
ordering point per channel (the Postgres `msg_serial`) and ensures every
node's linked list reflects the same global order.

NOTIFY's 8KB payload limit and at-least-once delivery are why we send
*pointers* (row IDs) rather than full payloads — the listener always reads
the canonical row from the table, deduplicating by `(channel, msg_serial)`.

LISTEN/NOTIFY's well-known throughput ceiling is not a concern here:
ably-server targets developer-loop, CI, and modest single-region self-host
deployments. Operators who need cloud-scale throughput should use Ably or
fork.

## 8. Identifiers & ordering

- **connectionId**: 12-char base64 of random 9 bytes, generated on `CONNECTED`.
  Process-local; never persisted, never recoverable.
- **clientId**: optional, resolved at auth time per §3.2. The server stamps
  it onto every outbound `Message.clientId` published by this connection,
  and rejects inbound frames that try to set a different value.
- **msgSerial** (channel): monotonic `BIGINT` from the storage backend
  (sequence/`MAX+1` under transaction, or atomic counter in memory).
- **Message.id**: `{connId}:{publishMsgSerial}:{messageIndex}` per Ably
  convention, so client-side de-dup works.
- **channelSerial**: on `ATTACHED`, the **confirmed attach point** — the
  serial from which the client's subscription begins. On each subsequent
  `MESSAGE`, the serial of that message. The client retains the most recent
  serial it has seen and sends it back on a future `ATTACH` to request
  continuation from that point (see §4.3).

Replay on `ATTACH` is a bounded history read from storage between the
client-supplied `channelSerial` and the channel's current head, streamed
after the `ATTACHED` ack. If the requested serial is older than retained
history the server attaches at the live head, clears
`ATTACHED.flags.RESUMED`, and populates `ATTACHED.error` so the SDK can
surface the discontinuity.

## 9. Configuration

CLI flags (each with an `ABLY_SERVER_*` env var equivalent):

```
--mode {memory|disk|cluster}      default: memory
--listen :8080                    HTTP/WS bind
--tls-cert / --tls-key            optional inline TLS
--api-key                         appId.keyId:keySecret
--data-dir ./data                 disk mode only
--db-dsn  postgres://…            cluster mode only
--message-ttl 2m
--max-messages-per-channel 1000
--shutdown-grace 10s              window to disconnect existing connections on SIGTERM
--log-level info
--log-format {text|json}
```

Loaded in priority order: flag > env > defaults. No config file.

## 10. Observability

- **Logs**: structured (`slog`), `text` for dev, `json` for prod.
- **Metrics**: Prometheus at `/metrics`. Counters for connections, attaches,
  messages in/out, REST requests; histograms for connection lifetime, publish
  latency.
- **Tracing**: OpenTelemetry, off by default, enabled by `OTEL_*` env.
- **pprof**: behind `--debug-listen` on a separate port.

## 11. Lifecycle & operations

On SIGTERM the server enters a graceful shutdown:

1. Stop accepting new WebSocket and HTTP connections.
2. Send `DISCONNECTED` to every existing WebSocket so SDKs reconnect
   elsewhere.
3. Wait up to `--shutdown-grace` (default `10s`) for connections to close
   themselves; after the window, any remaining connections are forcibly
   closed.
4. Drain in-flight REST handlers, then close storage.

In `cluster` mode each node is fungible. Rolling restart works because
clients are told to reconnect; the next node accepts the new connection
and, on each `ATTACH`, replays missed messages from Postgres using the
client-supplied `channelSerial`. No connection state crosses nodes.

## 12. Testing strategy

- **Unit**: per-package; mock-free where practical (the storage interface
  has an in-memory implementation, exercised by the same test suite as the
  SQLite and Postgres backends — table-driven contract tests).
- **Integration**: spin up the binary against ably-go's existing test suite
  (or a curated subset) to validate SDK compatibility.
- **Cluster**: Postgres + 2 server processes in Docker Compose; tests cover
  cross-node publish and `channelSerial`-based replay on reconnect to a
  different node.

There is no existing Ably protocol conformance suite to target; the
ably-go integration tests are the de-facto external check on SDK
compatibility.

## 13. Project layout & licensing

- License: **Apache 2.0** (matches ably-go).
- Module: `github.com/ably/ably-server`.
- Go version floor: **1.25**.
