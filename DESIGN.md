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

An attachment is a per-node (connection, channel) pair, structured as
a cursor over the Channel's linked list of entries (§5.1). A single
goroutine per attachment walks the cursor — parking on the current
entry's `notify` until the next entry is linked, then advancing and
forwarding the ChannelMessage. `forward` writes one `MESSAGE`
`ProtocolMessage` per ChannelMessage onto the connection's outbound
queue (with `ChannelSerial = cm.ChannelSerial`, `Messages =
cm.Messages`, gated by mode flags). The connection's single writer
goroutine (§5.2) serialises actual frame writes. There is no
per-attachment buffered fan-out channel: each attachment proceeds at
its own pace.

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
fan-out channels and no per-attachment buffering. Each list entry is one
ChannelMessage (§8) — an atomic publish carrying one or more Messages.

Channel is intentionally storage-agnostic. Serial minting and
persistence live in the storage backend (§6, §8); Channel exposes a
single `Append(cm)` method that links an already-minted ChannelMessage
onto the tail and wakes parked streams. Publish-path callers (the
realtime connection, the REST publish handler, the cluster broker's
NOTIFY listener) ask storage for a `cm` first and then pass it to
`Append`; see §7.

Each entry holds one ChannelMessage plus a `notify` channel that is
closed once the next entry is linked; parked attachment goroutines wake
on that close. The list is grow-only — older entries become eligible
for GC once no attachment retains a reference (see Memory below).

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
authorises the publish and runs the §7.1 sequence: it asks storage for
a minted+persisted ChannelMessage and, on a non-idempotent return,
links it into the local Channel (in cluster mode the local link is
driven instead by the `NOTIFY` listener — see §7.2). The connection
then replies with `ACK` / `NACK`. Inbound `ATTACH` / `DETACH` are
handled by the connection itself, calling into `ChannelManager` to
get/release a Channel.

## 6. Storage

The storage interface has two facets: a process-wide `Storage` that
hands out per-channel `ChannelStore`s and owns any shared resources
(e.g. a bolt DB handle), and a per-channel `ChannelStore` exposing two
operations:

- `AppendChannelMessage(ctx, msgs)` — mints a `channelSerial`, stamps
  each `Message.Serial = "<channelSerial>:<idx>"`, persists the
  resulting ChannelMessage atomically, and returns it. If any contained
  `Message.id` was already seen on this channel within the retention
  window, the call is idempotent: the originally-persisted
  ChannelMessage is returned with `idempotent=true` and no new row is
  written.
- `History(ctx, query)` — bounded forward range scan ordered by
  channelSerial; backs both the REST history endpoint and attachment
  resume gap-fills (§4.3).

Serial minting and idempotency live behind this interface so persistent
backends (bbolt, Postgres) can restore monotonic generator state across
restarts alongside the data it secures (§8). The canonical Go
signatures live in `internal/storage/storage.go`.

### 6.1 Memory backend

Per-channel ring buffer for messages, bounded by count *and* age.

### 6.2 Disk backend

[bbolt](https://github.com/etcd-io/bbolt) — a pure-Go embedded B+tree
KV store with crash-safe writes. Single file at `--data-dir/ably.db`.
Chosen because the disk backend's job is narrow ("survive crashes for
a single process") and bbolt gives us that without coupling the disk
layer's schema to the Postgres cluster backend.

Layout:

- One top-level bucket per channel, named `ch/<channel-name>`.
- Inside each channel bucket, a `messages` sub-bucket keyed by
  `channelSerial` (see §8 — the atomic-publish identifier
  `<timestamp>-<counter>@<seriesId>`). Values are the encoded
  `protocol.ChannelMessage` blob (msgpack), which carries the channel
  serial and the slice of contained messages. bbolt's natural
  byte-order iteration yields ChannelMessages in publish order.
- An `ids` sub-bucket maps `Message.id` (client-supplied idempotency
  key) to the `channelSerial` it landed in, so a repeat publish within
  the retention window can short-circuit and return the original
  serial without re-appending. Entries are dropped by the same sweep
  that trims `messages` past TTL — idempotency is bounded by message
  retention.

Plus a single top-level `_meta` bucket (not under any channel) holding
the process-wide serial generator state: the last-issued `(timestamp,
counter)` pair and the `seriesId`. Counter monotonicity is
per-`(timestamp, seriesId)` — i.e. process-wide, not per-channel —
because every minted serial on this node shares the same `seriesId`
and must be unique across all channels. On startup the process loads
this state to keep serials monotonic across restarts.

Retention is enforced by a background sweep goroutine that, per
channel, walks the ordered `messages` keys from oldest forward and
deletes anything past the message TTL or beyond the per-channel cap
(the cap counts ChannelMessages, since each is the unit of an atomic
publish). Because keys are serial-ordered and writes are append-only,
the sweep stops at the first non-expired key per channel.

bbolt has no native TTL, no secondary indexes, and a single-writer
model — all of which suit this use case: short-lived data, one writer
per node (the publish path), and the only read pattern beyond the
live tail is a bounded history range scan.

Future materialised state (e.g. presence membership when it lands)
will reuse this backend by adding sibling sub-buckets (e.g.
`presence`) under each channel bucket — separate from `messages`, no
TTL, with explicit deletes on `leave`.

### 6.3 Database backend (cluster mode)

Postgres only. LISTEN/NOTIFY gives us pub/sub and the same connection
serves as the durable store, so cluster mode needs nothing beyond a
single Postgres.

Schema sketch:

```sql
-- One row per individual Message. The PK groups Messages under their
-- shared channelSerial; the channelSerial prefix encodes the mint
-- timestamp (§8), so a forward range scan over the PK covers ordered
-- history reads AND time-based retention without a separate
-- created_at column.
CREATE TABLE messages (
  channel        TEXT  NOT NULL,
  channel_serial TEXT  NOT NULL,    -- "<ts>-<ctr>@<series>" (§8)
  idx            INT   NOT NULL,    -- position within the publish batch
  id             TEXT,              -- nullable, client-supplied idempotency key
  payload        BYTEA NOT NULL,    -- msgpack-encoded protocol.Message
  PRIMARY KEY (channel, channel_serial, idx)
);

-- Idempotency: a non-NULL client-supplied id is unique per channel
-- within the retention window. The partial index skips NULL ids so
-- publishes without an id never collide.
CREATE UNIQUE INDEX messages_idempotency_idx
  ON messages (channel, id) WHERE id IS NOT NULL;
```

The DDL ships in `internal/storage/postgres/schema.sql` and is applied
(CREATE … IF NOT EXISTS) when `postgres.Open` is called. Concurrent-
safe application across N nodes booting against an empty database —
plus a `schema_migrations` tracker for forward migrations — lives in
the auto-migrate path (DESIGN.md §11 / TASK-23).

Per-publish writes run inside a transaction that takes a per-channel
advisory lock (`pg_advisory_xact_lock(hashtext(channel))`), so
concurrent writers serialise per channel without contending across
channels. Each node generates its own seriesId at process start; the
serial format itself (the `@seriesId` suffix) disambiguates concurrent
mints, so generator state is not shared across nodes.

Retention is a periodic background job that deletes the oldest
messages on each channel using the channelSerial range — the first 14
characters are the zero-padded mint timestamp in ms, so lex
comparison matches numeric comparison:

```sql
DELETE FROM messages
WHERE channel = $1
  AND channel_serial < lpad(((now_ms - ttl_ms))::text, 14, '0');
```

A per-channel cap (counted in ChannelMessages = `DISTINCT
channel_serial`) is applied in the same sweep.

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

There is no pub/sub abstraction. A publish is a direct sequence run
inside the publish-path caller (the REST handler or the realtime
connection):

1. Authorise.
2. `storage.Channel(name).AppendChannelMessage(ctx, messages)` —
   atomically:
   - checks any contained `Message.id` against this channel's
     idempotency index; on hit, returns the originally-persisted
     ChannelMessage with `idempotent=true` and no new row is written;
   - otherwise mints a fresh `channelSerial` (§8), stamps each
     `Message.serial = channelSerial + ":" + idx`, persists the
     resulting `ChannelMessage{channelSerial, messages}` as one unit,
     and indexes any contained `Message.id`.
3. On a non-idempotent return, the caller calls
   `manager.GetChannel(name).Append(cm)` to link the cm into the
   live list. Attachments parked on the previous tail's `notify` wake
   up and observe the new entry.

`core.Manager` and `storage.Storage` are separate dependencies; the
publish-path caller holds both. All attachments are on the same node
and tail the same list.

### 7.2 Cluster mode (Postgres broker)

Each node runs a single goroutine on a dedicated Postgres connection
listening for channel-publish notifications:

- The publishing node mints the channel serial locally (§8), `INSERT`s
  the row into `messages` with that serial as primary key, and emits
  `NOTIFY ably_channel, '<channel-name>:<serial>'`.
- Every listening node (including the publisher) receives the
  notification, fetches the canonical ChannelMessage by
  `(channel, serial)`, and calls `channel.Append(cm)` on its local
  Channel.

The serial's format is itself the global ordering: the `<seriesId>`
component disambiguates serials minted in the same millisecond by
different processes, so each node's linked list, sorted lexicographically
by serial, reflects the same global order without a central sequence.

NOTIFY's 8KB payload limit and at-least-once delivery are why we send
*pointers* (channel + serial) rather than full payloads — the listener
always reads the canonical row from the table, deduplicating by
`(channel, serial)`.

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
- **msgSerial** (per-connection publish counter): `int64` on
  `ProtocolMessage`, assigned by the client; the server echoes it on
  `ACK`/`NACK` so the SDK can address publish acknowledgements. Distinct
  from `channelSerial` below — this is the wire field for publish flow
  control, not the canonical message ordering identifier.
- **ChannelMessage**: the atomic unit of a publish — one inbound
  REST request, or one `MESSAGE` frame carrying `messages[]`, lands
  on a channel as exactly one ChannelMessage containing one or more
  contained Messages. It is also the unit subscribers observe on the
  wire (one outbound `MESSAGE` frame per ChannelMessage) and the
  unit storage persists.
- **channelSerial** (atomic-publish identifier): a
  lexicographically-sortable string assigned by the server when a
  ChannelMessage lands on a channel, modelled on Ably cloud's
  internal format:

  ```
  <timestamp>-<counter>@<seriesId>
  ```

  - `timestamp` — current wall-clock time in milliseconds, zero-padded
    to 14 digits (~3000 years of headroom).
  - `counter` — zero-padded 3-digit per-`(timestamp, seriesId)` counter,
    incremented when multiple serials are minted within the same
    millisecond. Resets to `000` when the timestamp advances.
  - `seriesId` — a fixed-length random string generated at process
    start; disambiguates serials minted in the same millisecond on
    different nodes in cluster mode.

  channelSerials are the discrete attach/resume points in a channel's
  stream. On `ATTACHED` the wire field carries the confirmed attach
  point — the channelSerial from which the client's subscription
  begins. On each subsequent outbound `MESSAGE` frame, it carries the
  channelSerial of the ChannelMessage being delivered. The client
  retains the most recent channelSerial it has seen and sends it back
  on a future `ATTACH` to request continuation from that point (see
  §4.3).

  Lexicographic comparison of channelSerials matches publish order,
  which lets the disk backend (bbolt) and cluster backend (Postgres)
  use channelSerial directly as the primary key without a separate
  ordering column.
- **Message.serial**: the server-assigned identifier for an
  individual message within a ChannelMessage:

  ```
  <channelSerial>:<idx>
  ```

  where `idx` is a zero-padded 3-digit position within the
  ChannelMessage (`000` for a single-message publish). All messages
  in a batch share the channelSerial prefix and differ only by `idx`.
- **Message.id**: optional, **client-supplied** identifier used for
  idempotent publishing. If present, the server enforces uniqueness
  per channel within the message retention window: a second publish
  whose ChannelMessage contains an `id` already seen on this channel
  returns the original publish's `channelSerial` and is *not*
  re-appended. If absent, the server treats every publish as new.
  `id` is opaque to the server — clients typically use a UUID or a
  deterministic hash of payload + intent.

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
  bbolt and Postgres backends — table-driven contract tests).
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
