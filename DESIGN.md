# ably-server — Design

`ably-server` is a single Go binary that speaks Ably's realtime WebSocket
protocol and the core REST API, so existing Ably SDKs connect to it
unchanged. It covers pub/sub messaging — including presence and mutable
messages — and runs as one in-memory or on-disk process for local
development and CI, or as a cluster of stateless nodes over a shared
Postgres for self-hosted single-region deployments.

This document describes how it works, section by section. The task list in
[`backlog/`](backlog/) tracks implementation progress.

## 1. Non-goals

- Multi-region / global distribution.
- Ably-cloud-only product surface: integrations / rules, push notifications,
  Spaces, Chat, LiveObjects/LiveSync, message queues, account/app management
  APIs, the `/keys` admin API.
- Statistics collection. `GET /stats` exists purely as a compatibility
  stub — authenticated like any other REST read, gated by the app-wide
  `stats` op (§3.1), always returning an empty array — so SDK flows that
  call it succeed against this server. `POST /stats` is accepted as a
  matching no-op (same auth, drains the body, empty `201`): SDK test flows
  write stats before reading them, and the SDK's write path treats a
  non-2xx as an error whose body it reads, so a `404` would leave it
  blocked. No statistics are collected or stored.
- Hard durability or HA guarantees beyond what the chosen database provides.
- Backwards compatibility with arbitrary historical Ably protocol versions —
  we target v2 and later.
- Realtime transports other than WebSocket. Comet/HTTP-streaming and SSE
  are restricted-network fallbacks that do not apply to a local dev server
  or a deployment inside the operator's own network; SDKs use WebSocket.
- Token revocation. Revocable tokens presuppose per-token server-side
  state the single-app model does not keep.

## 2. External surface

### 2.1 Realtime (WebSocket)

`GET /` upgrades to WebSocket. The upgrade request carries:

- **Authentication** — via `Authorization` header or `key` / `access_token`
  query parameter (see §3).
- **Protocol version** — required `v=N` query parameter; the server accepts
  `v=2` and above and rejects older versions with an `ERROR` frame and
  close.
- **Format** — `format` query parameter selects `json` (text frames,
  default) or `msgpack` (binary frames).
- **Echo** — `echo` query parameter (default `true`). When `false`, the
  fan-out does not deliver a connection's own published `MESSAGE`s back to
  it; a message is identified as the connection's own by its stamped
  `connectionId`. Presence deliveries are unaffected — a connection always
  receives its own presence messages.

Each frame is a single `ProtocolMessage`. The server emits `CONNECTED` as
the first frame after a successful upgrade.

The `CONNECTED` frame carries a `connectionDetails` object alongside the
top-level `connectionId`, telling the SDK its resolved identity and the
limits to adopt for the connection:

- `clientId` — the resolved `clientId` (§3.2): a concrete value, `*` for a
  wildcard bearer, or omitted for an anonymous connection.
- `connectionKey` — the opaque key an SDK resumes with. Connection-state
  resume is a non-goal (§1, §11), so it is the process-local
  `connectionId` and is not recoverable.
- `maxMessageSize` — `65536` (64 KiB), the largest payload of a single
  publish; SDKs reject oversize publishes client-side.
- `maxFrameSize` — `524288` (512 KiB), the largest WebSocket frame / POST
  body.
- `maxInboundRate` — `1000`, the advisory per-connection publish ceiling
  in messages/second.
- `connectionStateTtl` — `120000` ms, how long an SDK treats the
  connection state as recoverable after an abrupt disconnect.
- `maxIdleInterval` — the server heartbeat cadence in milliseconds (the
  longest the server leaves the server→client direction idle before
  emitting `HEARTBEAT`).

The limits are advisory: the server publishes them for SDK consumption but
does not itself enforce them yet.

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
| `PRESENCE` (14) | ✓ | ✓ | presence enter/update/leave + delivery (see §12) |
| `MESSAGE` (15) | ✓ | ✓ | publish + delivery |
| `SYNC` (16) | | ✓ | presence set sync after attach (see §12) |
| `ANNOTATION` (21) | ✓ | ✓ | annotation publish + delivery (see §14) |
| `AUTH` (17) | ✓ | ✓ | inband re-auth: the server prompts near token expiry; the client supplies a fresh token (see §3) |

### 2.2 REST

All REST endpoints live under the root and accept either `application/json` or
`application/x-msgpack` for both request and response bodies (driven by
`Accept` / `Content-Type`).

| Method | Path | Purpose |
|---|---|---|
| POST | `/channels/{channel}/messages` | publish (create) 1..N messages |
| PATCH | `/channels/{channel}/messages/{serial}` | update / delete / append (action in body, see §13) |
| GET | `/channels/{channel}/messages` | history (paginated; latest version per message, see §13.4) |
| GET | `/channels/{channel}/messages/{serial}` | one message, latest version (see §13.4) |
| GET | `/channels/{channel}/messages/{serial}/versions` | all versions of a message (paginated) |
| POST | `/channels/{channel}/messages/{serial}/annotations` | publish an annotation on a message (see §14) |
| GET | `/channels/{channel}/messages/{serial}/annotations` | a message's annotations (paginated, see §14.4) |
| GET | `/channels/{channel}/presence` | current presence members (see §12) |
| GET | `/channels/{channel}/presence/history` | presence history (paginated) |
| POST | `/keys/{keyName}/requestToken` | mint a token (JWT) from a signed `TokenRequest` (see §3) |
| GET | `/stats` | compatibility stub: always an empty array (see §1) |
| POST | `/stats` | compatibility no-op: accepts and discards, empty `201` (see §1) |
| GET | `/time` | server time (ms since epoch) |
| GET | `/healthz` | liveness — no auth, dependency-free, 200 once serving |
| GET | `/readyz` | readiness — no auth; 200 in `memory`/`disk` mode; in `cluster` mode pings Postgres and returns 503 if unreachable |

A successful publish returns `201` with a `{"channel": "<name>",
"messageId": "<id>", "serials": ["<serial>", …]}` body (msgpack when the
`Accept` header requests it). `messageId` is the stamped id of the
publish's first message — `"<batchID>:0"` (§8) — the same id carried on
the delivered `MESSAGE` frame. `serials` carries one entry per published
message, in batch order, each the message's stable identity `serial` (§8)
— the value a client uses to address the message via `PATCH` / `GET
.../messages/{serial}` (§13), and what the SDK's `PublishWithResult`
surfaces.

Pagination follows Ably's `Link` header convention (`first`, `next`), each
rel emitted as its own `Link` header line with a URL relative to the
requested resource (its final path segment plus query), matching how Ably
SDKs resolve continuation links.

An unknown resource — an unrecognised path, or a known path under a method
Ably treats as a missing resource rather than a method error (e.g. `GET` on
the `POST`-only `requestToken`) — returns `404` with the Ably error body
`{"error":{"code":40400,"statusCode":404,"message":...}}` in the `Accept`
format, alongside the `X-Ably-Errorcode` / `X-Ably-Errormessage` headers
SDKs read for the code and message.

## 3. Authentication & authorisation

One or more API keys are configured (via repeated `--api-key`, a
comma-separated `ABLY_SERVER_API_KEY`, or the config file — §9), each in
the canonical Ably format `appId.keyId:keySecret` (so SDKs that parse the
key work unchanged). Each key carries a **capability** (§3.1): a key
configured via a flag, an env var, or a bare `api-key`/`api-keys` entry
grants the full `{"*":["*"]}` capability, while a structured `[[keys]]`
config entry (§9) may narrow it to a scoped set. A Basic-auth holder of a
key resolves to that key's capability, and a token minted or signed by a
key is bounded by it (§3.3). At least one key is required; the server
refuses to start with none. All configured keys must share the same
`appId`: the
server owns one channel namespace (mirroring how one Ably app owns one
namespace), so keys spanning multiple appIds are a misconfiguration and
startup fails. A request authenticates against **any** configured key,
and JWT verification selects the signing key by the token's `kid` header
(falling back to trying every key's secret when `kid` is absent or names
no configured key). The server **verifies** tokens presented on
connect/request and also **issues** them on demand via
`POST /keys/{keyName}/requestToken` (§3.3).

Two accepted credential forms:

1. **Basic auth** — `Authorization: Basic base64(appId.keyId:keySecret)` or
   `?key=...`. Carries the API key's full capability.
2. **JWT** — bearer token signed with `keySecret` (HS256). Passed on REST as
   `Authorization: Bearer base64(jwt)` (Ably base64-encodes the header value,
   RSA3a — a raw token is also accepted) or, on the WS upgrade, as the
   `?access_token=...` query param (`?accessToken=...` is also accepted).

JWT claims:

| Claim | Required | Purpose |
|---|---|---|
| `iat` | ✓ | issued-at; a token issued in the future beyond a small clock-skew leeway is rejected |
| `exp` | ✓ | expiry; checked with no grace, so an expired token is rejected the instant it lapses |
| `x-ably-capability` | | JSON object granting per-channel ops (see §3.1). Present → the token's capability is the claim **intersected with** the signing key's capability. Absent → the token inherits the signing key's capability outright (for a full-capability key that is `{"*":["*"]}`, so the claim is only needed to *narrow* access; for a scoped key the token is bounded by the key regardless) |
| `x-ably-clientId` | | string; controls the connection's `clientId` (see §3.2) |

**Inband re-authentication.** A token-authenticated WebSocket tracks its
token's `exp`. About 30 seconds before expiry the server sends an `AUTH`
frame prompting the client to renew (immediately if the token is adopted
already inside that window); the client replies with an `AUTH` frame
carrying a fresh token in `auth.accessToken`. The server verifies it and
requires the new credential to be **compatible** with the connection — the
resolved `clientId` (§3.2) must be unchanged — then swaps in the new
capability set and expiry and replies with a `CONNECTED` frame carrying
updated `connectionDetails`, all without dropping the connection.

A token whose remaining lifetime on adoption is below a small margin (~5.5
seconds) is **not** prompted: it is simply left to expire. The two failure
modes are deliberately distinct so the SDK reacts correctly:

- **Token lapsed** (no valid token by `exp`) — the server sends a
  `DISCONNECTED` frame with the token-expired code `40142` (status `401`).
  This is renewable: the SDK obtains a fresh token and reconnects (RTN15h2,
  RTN22a).
- **Client-supplied token rejected** (an inband `AUTH` whose token fails to
  verify, carries no token, or resolves to an incompatible `clientId`) — the
  server sends an `ERROR` frame with a non-renewable credential error
  (`40101` invalid credentials, `40102` incompatible; status `401`). The
  code sits outside the SDK's renewable token-error range, so the SDK moves
  the connection to `FAILED` rather than looping on reconnect (RTC8a2).

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
  A resource may carry a leading `[qualifier]` prefix scoping the resource
  TYPE (`[qualifier]name`); the qualifier is parsed and matched per Ably
  semantics. `[*]` matches any type, so the standard sandbox all-access key
  `{"[*]*":["*"]}` grants everything a plain channel needs (equivalent to
  `{"*":["*"]}` over the channel surface here). A concrete qualifier such
  as `[queue]*` / `[meta]*` parses but matches no channel, since neither
  queues nor metachannels exist as resources. When intersecting
  capabilities (§3.3) a `[*]` qualifier on either side yields the other's,
  and two differing concrete qualifiers do not intersect.
- `<op>` is one of `publish`, `subscribe`, `presence`, `history`,
  `stats`, `annotation-publish`, `annotation-subscribe`,
  `message-update-own`, `message-update-any`,
  `message-delete-own`, `message-delete-any`. `*` matches any op.
  The `annotation-*` ops gate annotations (§14): publishing one, and
  receiving the raw `ANNOTATION` stream (summaries need only
  `subscribe`).
  `stats` gates the `/stats` stub (§1) and is app-wide rather than
  per-channel, so it must be granted on the `*` resource.
  `subscribe` covers both
  receiving messages and receiving presence (events + sync + the current
  set); `presence` covers registering presence (enter/update/leave);
  `history` covers message history, message version history, and
  presence history. The `message-{update,delete}-{own,any}` ops gate
  mutable messages (§13.5): `-own` permits the operation only when the
  caller's resolved `clientId` matches the target message's creator,
  `-any` waives that check; `append` is gated by `message-update-*`.

Every authenticated request resolves to a **capability set**. For each
operation the server computes the union of granted ops across all matching
resources and checks that the requested op is in the union. If not, the
operation is rejected:

| Surface | Op required |
|---|---|
| WS `ATTACH` flag `SUBSCRIBE` | `subscribe` |
| WS `ATTACH` flag `PUBLISH` | `publish` |
| WS `ATTACH` flag `PRESENCE` | `presence` |
| WS `ATTACH` flag `PRESENCE_SUBSCRIBE` | `subscribe` |
| WS inbound `MESSAGE` (`create`) | `publish` (and the attachment must hold the `PUBLISH` mode flag, granted at attach time) |
| WS inbound `MESSAGE` (`update`/`append`) | `message-update-own`/`-any` (see §13.5) |
| WS inbound `MESSAGE` (`delete`) | `message-delete-own`/`-any` |
| WS inbound `PRESENCE` | `presence` (and the attachment must hold the `PRESENCE` mode flag) |
| WS `ATTACH` flag `ANNOTATION_PUBLISH` | `annotation-publish` |
| WS `ATTACH` flag `ANNOTATION_SUBSCRIBE` | `annotation-subscribe` |
| WS inbound `ANNOTATION` | `annotation-publish` (and the attachment must hold the `ANNOTATION_PUBLISH` mode flag) |
| REST `POST .../messages` | `publish` |
| REST `PATCH .../messages/{serial}` | `message-{update,delete}-{own,any}` (see §13.5) |
| REST `GET .../messages`, `GET .../messages/{serial}[/versions]` | `history` |
| REST `GET .../presence` | `subscribe` |
| REST `GET .../presence/history` | `history` |
| REST `POST .../messages/{serial}/annotations` | `annotation-publish` |
| REST `GET .../messages/{serial}/annotations` | `history` |
| REST `GET /stats` | `stats` — app-wide, so it must be granted on the `*` resource |

`ATTACH` mode resolution: the effective mode set delivered in `ATTACHED.flags`
is `requested ∩ capability-permitted`. Empty intersection → `ERROR` with
`code: 40160` (insufficient capabilities) and the channel is not attached.

### 3.2 Client ID

A connection (or REST request) resolves to one of three identities:

- **concrete** `clientId` — the server stamps it onto every outbound
  `Message.clientId` that omits one, and rejects (`NACK`) any inbound
  message asserting a *different* one;
- **wildcard** (`*`) — the bearer may assume any identity, chosen per
  operation: each message carries its own `clientId`, stamped through
  unchanged, and a message with none stays unidentified;
- **none** (anonymous) — the bearer may assert no identity; a message
  carrying any `clientId` is rejected.

Resolution depends on the credential and the `clientId` query parameter on
the upgrade (WS) or request (REST):

| Credential | `x-ably-clientId` claim | `clientId` query param | Resolved `clientId` |
|---|---|---|---|
| Basic | n/a | absent | `*` (wildcard — a key may assume any identity) |
| Basic | n/a | `<value>` | `<value>` |
| JWT | absent | absent | none (anonymous) |
| JWT | absent | `<value>` | rejected (no permission to assert clientId) |
| JWT | `<concrete>` | absent | `<concrete>` (claim value) |
| JWT | `<concrete>` | `<concrete>` matching claim | `<concrete>` |
| JWT | `<concrete>` | `<value>` ≠ claim | rejected |
| JWT | `*` | absent | `*` (wildcard) |
| JWT | `*` | `<value>` | `<value>` |

`*` is a wildcard marker meaning "the bearer may assume any `clientId`". It
is **never** itself used as a `clientId` on a message or as a member
identity; to act under a fixed identity the bearer selects a concrete
value via the `clientId` query parameter.

A connection or REST request that fails the table above is rejected at
auth time — the WS upgrade or REST request returns `401`.

Presence imposes a further requirement at *use* time rather than auth
time: a member must be identified, so a connection that resolved to no
`clientId` (anonymous) cannot enter presence, and a wildcard bearer must
select a concrete `clientId` to enter (see §12.3).

### 3.3 Token requests

`POST /keys/{keyName}/requestToken` mints a token for a key holder. The
body is an Ably `TokenRequest` (`keyName`, `ttl`, `capability`, `clientId`,
`timestamp`, `nonce`, `mac`); `ttl` and `timestamp` are accepted as either
a JSON number or a numeric string (the `authUrl` exchange reassembles a
signed request from query params, so they arrive as strings). The request
is authenticated one of two ways:

- **Signed** — the `mac` is `base64(HMAC-SHA256(text, keySecret))` over the
  canonical text `keyName"\n" ttl"\n" capability"\n" clientId"\n"
  timestamp"\n" nonce"\n"` (`ttl` empty when unset). The server recomputes
  it and compares in constant time.
- **Basic, same key** — a request without a `mac` is accepted only when it
  also carries Basic auth for the same key (the holder is explicitly
  authenticated, so the mac is unnecessary).

An authenticated request is then validated (client errors, not server
errors):

- **`capability`** — must be well-formed: parseable JSON, every op list
  non-empty, `*` never combined with another op, and every op a recognised
  Ably operation name. A violation is a `400` (`40000`). (A configured
  key's own capability is trusted and not subject to this check, so it may
  carry ops this server does not itself enforce, e.g. push.)
- **`ttl`** — a negative ttl or one above the 24-hour maximum is a `400`; a
  non-numeric ttl is a `400`. Zero/absent means the 60-minute default.
- **`timestamp`** — must be within two minutes of server time, else `40104`
  (`401`) — guarding against stale or replayed requests.
- **`nonce`** — a nonce reused within the timestamp window is a replay,
  rejected with `40105` (`401`). Seen nonces are tracked in-memory and
  evicted once past the window (a later reuse fails the timestamp check
  regardless).

The minted token is an **HS256 JWT** signed with the key secret, `kid` set
to the key name, carrying `iat`/`exp` (from `ttl`, default 60 min), the
request `nonce` as `jti` (so distinct requests yield distinct tokens), and
`x-ably-capability` / `x-ably-clientId` when supplied. `exp` carries
sub-second precision, so a short (sub-second) ttl yields a token that
actually expires when it should rather than lasting up to a whole second.
It is returned in a `TokenDetails` response body (`token` holds the JWT),
which the SDK then presents as an `access_token` verified by the path
above.

The requested `capability` is *narrowed* against the signing key's
capability before it is stamped on the token (§3.1): the token grants
only the intersection of what was requested and what the key permits. A
full-capability key (`{"*":["*"]}`) passes a requested capability through
unchanged; a scoped key clamps it to the key's own grants; either way a
request whose capability the key cannot grant at all (empty intersection)
is rejected with `40160` (`401`). A request with no capability leaves the
claim unset, so the minted token inherits the key's capability when it is
later verified. The `TokenDetails` response always carries the **granted**
capability as a JSON string — the narrowed value stamped on the token, or
the key's own capability verbatim when none was requested — so the SDK can
always parse it.

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

A repeat `ATTACH` for a channel the connection is already attached to is a
**re-attach**, not a no-op: the SDK sends `ATTACH` whenever the channel is
not locally `ATTACHED`/`ATTACHING` and blocks until it sees an `ATTACHED`
(RTL4). The server tears down the existing attachment and builds a fresh one
from the new `ATTACH`'s modes, cursor, and params, so the client always
receives a new `ATTACHED` (plus any replay).

`ATTACHED` is the **first** frame the server emits in response to `ATTACH`;
any replay or live messages follow it. Its fields:

- `flags` — the **effective** mode set (see §4.2). The SDK reads
  `channel.modes` from these bits (RTL4m).
- `channelSerial` — the **confirmed attach point**: the serial *from which*
  the message stream the client is now subscribed to begins. Subsequent
  `MESSAGE` frames carry their own serials advancing from that point, and
  the SDK uses the most recent serial it has seen as its cursor for any
  future re-`ATTACH`.
- `params` — the channel params the client requested, echoed back so the
  SDK can populate `channel.params` (RTL4k1). The `modes` entry, when the
  client requested modes via the params map, is rewritten to the effective
  mode set (§4.2); other requested params (e.g. `rewind`) pass through
  unchanged. A re-`ATTACH` (including one triggered by `setOptions`)
  rebuilds the attachment, so the new `ATTACHED` reflects the updated
  params and modes.

### 4.2 Modes

A client requests modes two ways: the `ATTACH.flags` bitfield, or a
comma-separated `modes` channel param (e.g. `params.modes =
"subscribe,presence"`, how ably-js sends `ChannelOptions.params.modes`).
The `modes` param takes precedence over the flags mode bits, which take
precedence over the default set; an unrecognised param token is ignored.
The mode bits occupy the high end of the flags word, matching Ably's wire
constants:

| Mode | Bit | Grants |
|---|---|---|
| `PRESENCE` | `1 << 16` | enter/update/leave presence (see §12) |
| `PUBLISH` | `1 << 17` | publish `MESSAGE` |
| `SUBSCRIBE` | `1 << 18` | receive `MESSAGE` |
| `PRESENCE_SUBSCRIBE` | `1 << 19` | receive `PRESENCE` + presence sync |
| `ANNOTATION_PUBLISH` | `1 << 21` | publish `ANNOTATION` (see §14) |
| `ANNOTATION_SUBSCRIBE` | `1 << 22` | receive raw `ANNOTATION` frames (see §14.3) |

If `flags` carries no mode bits the server treats it as the default set —
the four non-annotation modes above (matches SDK default; the annotation
modes are opt-in, §14.3).

The effective mode set is `requested ∩ capability-permitted`, where the
permitted set is derived from the per-op capability mapping in §3:

- `SUBSCRIBE` and `PRESENCE_SUBSCRIBE` permitted iff cap grants
  `subscribe` on the channel.
- `PUBLISH` permitted iff cap grants `publish`.
- `PRESENCE` permitted iff cap grants `presence`.
- `ANNOTATION_PUBLISH` / `ANNOTATION_SUBSCRIBE` permitted iff cap grants
  `annotation-publish` / `annotation-subscribe`.

Empty intersection → the attach is rejected with `ERROR` (`code: 40160`)
and no channel state is created. Otherwise `ATTACHED.flags` carries the
effective set, plus the `HAS_PRESENCE` flag (`1 << 0`) when the channel
has a non-empty presence set, so the SDK knows a `SYNC` will follow
(§12.4). When the modes were requested via the `modes` param, the
effective set is also echoed back in `ATTACHED.params.modes` (§4.1).

Once attached, modes gate frame flow:

- An attachment without `SUBSCRIBE` does not receive `MESSAGE` frames.
- An attachment without `PRESENCE_SUBSCRIBE` receives neither live
  `PRESENCE` frames nor the post-attach `SYNC`.
- Inbound `MESSAGE` from an attachment without `PUBLISH` is rejected with
  `NACK`.
- Inbound `PRESENCE` from an attachment without `PRESENCE` is rejected
  with `NACK`.

### 4.3 Replay (`channelSerial` and `rewind`)

The v2 protocol holds **no per-connection server state across disconnects**.
There is no recovery TTL, no retained outbox, no resume buffer. Connection-
level `recover` / `resume` parameters are accepted on the upgrade URL for
SDK compatibility but never resume connection state: a reconnect always
gets a **fresh `connectionId`**, so connection-state resume/recovery is an
accepted incompatibility (the SDK's connectionId-continuity checks —
ably-go RTN15c6, RTN16f — do not hold against this server).

The server still declines a resume/recover *per protocol* so the SDK reacts
cleanly rather than silently mis-accounting a phantom resume:

- A **malformed** `resume` / `recover` key (not a well-formed connectionId
  this server could have issued) yields `CONNECTED` with the fresh
  `connectionId` **plus an error** (`80018`, status `400`). Because the
  connectionId differs *and* an error is present, the SDK treats the resume
  as failed: it resets its `msgSerial`, resends pending publishes, and
  re-attaches its channels (RTN15c7, RTN16e).
- A **well-formed** key is left un-errored. The server cannot truly resume
  it (it holds no connection state) but does not know it from a genuine
  resume, so it starts a fresh connection without signalling failure; the
  SDK recovers message flow through per-channel re-attach (below) rather
  than connection resume.

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
  messages are no longer held in storage — see the retention note in §6)
  the server still attaches: it picks the channel's current head as the attach
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
and `rewind` is ignored. `rewind` is likewise suppressed when the `ATTACH`
is a resume — either a supplied `channelSerial` or the `ATTACH_RESUME`
flag (`1 << 5`, RTL4j, set by the SDK on a non-clean attach): a
continuation must not replay history the client has already seen.

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
its own pace, lagging the live tail with no upper bound but its own
memory footprint.

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

Three deployment modes are selected by configuration, differing only in
where state and pub/sub live:

1. `memory` — single process, in-memory state, in-memory pub/sub.
2. `disk` — single process, on-disk persistence, in-memory pub/sub.
3. `cluster` — N processes, shared database for both state and pub/sub.

Server processes are stateless: any node can serve any connection, with no
peer-to-peer membership or gossip. The mechanics of each mode are detailed
in §6 (storage) and §7 (pub/sub).

Protocol types (`ProtocolMessage`, `Action`, `Message`, `PresenceMessage`)
are defined in `internal/protocol/`, with their field tags and
action/flag constants pinned to ably-go's wire constants so SDKs
interoperate unchanged. (The original plan was to import
`github.com/ably/ably-go/ably/proto` directly and fall back to local
definitions only on friction; in practice the local definitions carry the
whole surface, so the package owns them outright.)

Major packages:

```
cmd/ably-server/        # main, flag/env wiring
cmd/ably-bench/         # pub/sub load benchmark
internal/protocol/      # wire types + json/msgpack codec; presence and mutable-message types
internal/auth/          # API-key parsing + Basic auth
internal/realtime/      # WebSocket upgrade, connection loop, attachment cursor, presence, mutation, rewind
internal/rest/          # HTTP handlers + router
internal/core/          # Channel + ChannelManager (live entry list)
internal/storage/       # Storage interface + memory / bbolt / postgres backends
                        #   (the postgres backend carries the cluster LISTEN/NOTIFY broker)
internal/serial/        # channelSerial minting + global ordering
internal/id/            # connection IDs, message IDs
```

### 5.1 Channel

A `Channel` is a per-node, per-name structure holding the live message
list. It has **no goroutine of its own**: concurrency is serialised by a
mutex around append. Attachments tail the list at their own pace, with no
fan-out channels and no per-attachment buffering. Each list entry is one
ChannelMessage (§8) — an atomic publish carrying one or more Messages.

Each Channel is paired at construction with its `storage.ChannelStore`
facet (Manager calls `storage.Channel(name, channelAsAppender)` so
the storage holds a back-link to the in-process Channel). Channel
exposes two methods:

- `Publish(ctx, msgs)` — orchestrates a publish: delegates to
  `store.Store(ctx, msgs)`, which mints the channelSerial and
  persists. The link onto the live list arrives via the Appender
  callback — synchronously after commit in memory/bbolt;
  asynchronously via the LISTEN goroutine in Postgres (§7).
- `Append(cm)` — satisfies the `storage.Appender` contract. It is
  the **only** writer to the linked list, and it is called only by
  the storage backend (never directly by publish-path callers, in
  any mode).

This is the unified flow: every cm that lands on a Channel's live
list arrives through `storage → Appender.Append`, whether the publish
originated locally or on a remote node.

Each entry holds one ChannelMessage plus a `notify` channel that is
closed once the next entry is linked; parked attachment goroutines wake
on that close. The list is grow-only — older entries become eligible
for GC once no attachment retains a reference (see Memory below).

The first `ATTACH` to a name (or the first publish) creates the Channel.
The Channel is removed from the manager only when **both** are true:

- it has no attachments, and
- every entry on its linked list has aged past the retention window (§6).

Holding the Channel for the retention window after the last detach keeps the
in-process list available to serve a fresh `ATTACH` that arrives soon
after with a `channelSerial` covering still-live messages, without having
to re-materialise the list from storage.

**Memory.** Go's GC reclaims entries once no attachment retains a
reference. A slow attachment retains the prefix of the list between its
cursor and the live tail, so memory grows with its lag. The retention
policy (§6) is intended to bound the working set: once a message ages past
the retention window or the per-channel `max_messages` cap, the Channel drops its own
back-pointer to it, so any unreferenced entries become eligible for GC.

### 5.2 Connection loop

Each WebSocket connection has:

- A read goroutine that decodes inbound frames and dispatches on `Action`.
- A write goroutine that serializes outbound frames (only one writer per
  connection per gorilla/websocket conventions).
- A heartbeat ticker that sends `HEARTBEAT` if idle.
- An `attachments map[string]*Attachment` keyed by channel name.

Inbound `MESSAGE` / `PRESENCE` is validated on the read goroutine
(clientId resolution, connectionId stamping, presence attachment/mode
checks) and then handed to a per-connection **publish worker** — a single
goroutine draining a FIFO queue of publish tasks — so the storage write
happens off the read goroutine and never blocks decoding of the next
frame. The worker calls `channel.Publish(ctx, msgs)` (or `Mutate` /
`PublishPresence`), which returns once storage has committed; the link
onto the local linked list happens asynchronously via the Appender
callback the storage holds (synchronous in memory/bbolt, NOTIFY-driven in
Postgres — see §7). Only then does the worker emit the frame's `ACK` (or
`NACK` on failure), echoing the publish `msgSerial`.

A single FIFO worker per connection is deliberate: it keeps this
connection's channel appends in publish order and emits `ACK` / `NACK` in
`msgSerial` order (the SDK correlates acknowledgements positionally, so an
out-of-order or premature ack corrupts its pending-publish accounting).
Validation rejections are enqueued through the same worker so their `NACK`
stays ordered behind any still-in-flight publishes.

An inbound `MESSAGE` / `PRESENCE` `msgSerial` must be monotonic per
connection. A frame whose `msgSerial` repeats or goes backward is a client
retransmit — e.g. the SDK re-flushing a queued publish with the same
`msgSerial` after a reconnect (RTL6c2) — and is **dropped** on the read
goroutine before it reaches the worker: it is neither re-published nor
re-`ACK`ed. A duplicate `ACK` for a `msgSerial` the SDK has already dequeued
would corrupt its positional accounting. A forward skip is accepted.

Inbound `ATTACH` / `DETACH` are handled by the connection itself on the
read goroutine, calling into `ChannelManager` to get/release a Channel.

## 6. Storage

> **Open design question — retention (TASK-26).** This section describes a
> retention *mechanism* (a serial-ordered sweep bounded by a message TTL and
> a per-channel `max_messages` cap), but the *policy* it enforces is not yet
> settled: the default TTL value, the cap, and whether operators can
> override either are still being decided. References elsewhere to "the
> retention window" or messages "aging out" point back here.

The storage interface has two facets: a process-wide `Storage` that
hands out per-channel `ChannelStore`s and owns any shared resources
(e.g. a bolt DB handle, a pgxpool), and a per-channel `ChannelStore`
exposing two operations:

- `Store(ctx, msgs)` — mints a `channelSerial` and persists the resulting
  ChannelMessage atomically, then returns it. For a `create` it stamps
  each `Message.serial = "<channelSerial>:<idx>"` (serial == version);
  for an `update`/`delete`/`append` the `serial` is the caller-supplied
  target and only `version` takes the new `<channelSerial>:<idx>` (§13.1).
  If any contained `Message.id` was already seen on this channel within
  the retention window, the call is idempotent: the originally-persisted
  ChannelMessage is returned with `idempotent=true` and no new row is
  written.
- `History(ctx, query)` — bounded forward range scan ordered by
  channelSerial; backs both the REST history endpoint and attachment
  resume gap-fills (§4.3). Messages and presence share one stream and
  one channelSerial namespace (§12.1), so the query carries a kind
  selector: a message-history scan skips presence cms and vice versa.
- `StorePresence(ctx, presence)` — the presence analogue of `Store`
  (§12.2): mints a channelSerial, stamps each `PresenceMessage.serial`,
  persists the presence cm onto the same stream, and in the same atomic
  step folds it into the channel's **membership set** (ENTER/UPDATE
  upsert the member keyed by `connectionId:clientId`, LEAVE removes it).
  The Appender then delivers the cm exactly as for a message publish.
- `Members(ctx)` — returns the current membership set plus the
  channelSerial it is current as-of; backs presence sync (§12.4) and the
  REST `GET .../presence` endpoint. The set is owned by the backend
  (§12.5): a map in memory, in-memory in bbolt, a `presence` table in
  Postgres.
- `Store` also handles **mutations** (§13): an `update`/`delete`/`append`
  is a publish whose Message carries an `action` and a target `serial`.
  The backend validates the target exists within retention, applies the
  shallow-mixin merge against the message's current latest version,
  persists the new version as an ordinary cm on the log, and updates two
  derived structures it owns — a **serial → versions** secondary index
  (backing version-history reads and target validation) and a
  **latest-version projection** per message serial (backing the collapsed
  `GET .../messages` and `GET .../messages/{serial}`). The projection is
  the message-side analogue of the presence membership set: a map in
  memory, a `messages` bucket in bbolt, a materialised `messages` table in
  Postgres; the versions index is a `versions` bucket / partial index over
  the log (§6.3).
- `History` therefore has two message modes: the default collapses to the
  latest version of each message positioned at its create serial; a
  by-serial version scan returns every version of one message ordered by
  `version` (§13.4).

Each `ChannelStore` is created with an `Appender` callback —
`Storage.Channel(name, appender) ChannelStore`. The Appender is the
single delivery path for committed cms: the backend invokes
`appender.Append(cm)` on every fresh publish (skipped on idempotent
returns, where the original was delivered when first persisted).
Memory and bbolt fire it synchronously after commit. Postgres fires
it asynchronously from the LISTEN goroutine after the NOTIFY emitted
inside the commit tx round-trips — including for the publisher's own
publish, so there is no separate path for self-publishes (§7).

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

Layout — channel-scoped buckets via composite keys:

- `channel_messages`: the append-only log, keyed `<channel>\0<channelSerial>`
  (see §8 — the atomic-publish identifier `<timestamp>-<counter>@<seriesId>`).
  Values are the msgpack-encoded `protocol.ChannelMessage` blob (a
  message or presence cm). bbolt's byte-order iteration over a
  `<channel>\0` prefix yields a channel's ChannelMessages in publish
  order, mirroring the Postgres backend's PK range scan.
- `ids`: keyed `<channel>\0<Message.id>`, value is the channelSerial
  the id landed in. bbolt has no secondary indexes, so this is the
  manual equivalent of Postgres's partial UNIQUE idempotency index.
  Entries are dropped by the same sweep that trims `channel_messages`
  past TTL — idempotency is bounded by message retention.
- `versions` and `messages` (mutable messages, §13): the bbolt analogue
  of Postgres's `channel_messages_serial_idx` and the materialised
  `messages` table. `versions` is keyed
  `<channel>\0<message_serial>\0<version_serial>` so a prefix scan
  enumerates a message's versions in order; `messages` is keyed
  `<channel>\0<message_serial>` and holds the merged latest version.
  Both are written by the same single writer as `channel_messages` and
  trimmed by the same retention sweep. (Unlike the presence membership
  set, these are durable — they are message state, not connection-scoped.)

Per-process `seriesId` is regenerated on every `Open` and generator
monotonic state is not persisted. The §8 serial format makes
post-restart monotonicity fall out naturally: the 14-character
zero-padded ms timestamp is the leading lex-comparison key, and wall
clock advances between restarts, so a post-restart Mint sorts after
all prior serials. The same-millisecond restart with an unlucky new
seriesId is the only edge case we don't guarantee, and we don't.

Retention is enforced by a background sweep goroutine that, per
channel, walks the ordered `channel_messages` keys from oldest forward and
deletes anything past the message TTL or beyond the per-channel cap
(the cap counts ChannelMessages, since each is the unit of an atomic
publish). Because keys are serial-ordered and writes are append-only,
the sweep stops at the first non-expired key per channel.

bbolt has no native TTL, no secondary indexes, and a single-writer
model — all of which suit this use case: short-lived data, one writer
per node (the publish path), and the only read pattern beyond the
live tail is a bounded history range scan.

### 6.3 Database backend (cluster mode)

Postgres only. LISTEN/NOTIFY gives us pub/sub and the same connection
serves as the durable store, so cluster mode needs nothing beyond a
single Postgres.

Schema sketch:

```sql
-- The append-only LOG: one row per individual Message or PresenceMessage,
-- both kinds interleaved in one channelSerial namespace (§12.1). The PK
-- groups a publish's Messages under their shared channelSerial; the
-- channelSerial prefix encodes the mint timestamp (§8), so a forward
-- range scan over the PK covers ordered history reads AND time-based
-- retention without a separate created_at column. action is NOT a column
-- — it lives in the payload (nothing scans by it); kind is, because reads
-- filter by it.
CREATE TABLE channel_messages (
  channel        TEXT     NOT NULL,
  channel_serial TEXT     NOT NULL,    -- "<ts>-<ctr>@<series>" (§8) — this cm's position
  idx            INT      NOT NULL,    -- position within the publish batch
  id             TEXT,                 -- nullable, client-supplied idempotency key
  kind           TEXT     NOT NULL DEFAULT 'message',  -- 'message' | 'presence' (§12.1) | 'annotation' (§14)
  message_serial TEXT,                 -- message identity (§8); NULL for presence; = channel_serial:idx for a create
  payload        BYTEA    NOT NULL,    -- msgpack protocol.Message or PresenceMessage, per kind
  PRIMARY KEY (channel, channel_serial, idx)
);

-- Idempotency: a non-NULL client-supplied id is unique per channel
-- within the retention window. The partial index skips NULL ids so
-- publishes without an id never collide.
CREATE UNIQUE INDEX channel_messages_idempotency_idx
  ON channel_messages (channel, id) WHERE id IS NOT NULL;

-- serial → versions: every version of a message in version order, backing
-- GET .../messages/{serial}/versions and update/delete target validation
-- (§13.4). Partial — message rows only; presence rows have no identity.
CREATE INDEX channel_messages_serial_idx
  ON channel_messages (channel, message_serial, channel_serial)
  WHERE message_serial IS NOT NULL;

-- Materialised current MESSAGE state: one row per live message holding its
-- latest version — the message-side analogue of the presence table below.
-- UPSERTed in the same transaction as the version's INSERT into
-- channel_messages; a delete sets deleted = true but the row (and its
-- versions in the log) stay queryable (soft delete, §13.2). create_serial
-- keeps the message in its original position for collapsed history.
CREATE TABLE messages (
  channel         TEXT    NOT NULL,
  message_serial  TEXT    NOT NULL,  -- stable identity
  create_serial   TEXT    NOT NULL,  -- the create's channel_serial:idx (ordering position)
  version_serial  TEXT    NOT NULL,  -- the latest version's channel_serial:idx
  deleted         BOOLEAN NOT NULL DEFAULT false,
  payload         BYTEA   NOT NULL,  -- msgpack of the merged latest Message
  PRIMARY KEY (channel, message_serial)
);

-- Materialised current PRESENCE state: one row per live member, keyed by
-- connectionId:clientId. ENTER/UPDATE upsert, LEAVE deletes — maintained
-- in the same transaction as the presence cm's INSERT into channel_messages,
-- so the set stays consistent with the log. node_id + expires_at drive the
-- crashed-node reaper (§12.5).
CREATE TABLE presence (
  channel        TEXT        NOT NULL,
  connection_id  TEXT        NOT NULL,
  client_id      TEXT        NOT NULL,
  channel_serial TEXT        NOT NULL,  -- serial of the latest ENTER/UPDATE
  payload        BYTEA       NOT NULL,  -- msgpack-encoded protocol.PresenceMessage
  node_id        TEXT        NOT NULL,  -- owning node, for lease bump + reap
  expires_at     TIMESTAMPTZ NOT NULL,  -- liveness lease; reaper deletes once past
  PRIMARY KEY (channel, connection_id, client_id)
);
```

Reads over the log (`channel_messages`) add `kind = 'message'` (or
`'presence'`) to the predicates above. Collapsed message history reads the
materialised `messages` table ordered by `create_serial`; a version scan
reads `channel_messages` via `channel_messages_serial_idx`. The retention
sweep deletes log rows by channel_serial range and, when a message's last
surviving version ages out, drops its `messages` projection row too. The
`presence` projection is independent of the log's retention sweep (§12.5).

The DDL ships as versioned migrations under
`internal/storage/postgres/migrations/*.sql` (e.g. `0001_initial.sql`)
and is applied at `postgres.Open` by an auto-migrate sweep:

1. Acquire a session-scoped `pg_advisory_lock` on a fixed int8 key
   (arbitrary — advisory-lock keyspace is per-database and opt-in).
   N nodes booting simultaneously block here; only one applies the
   migrations, the rest observe an up-to-date state and skip.
2. `CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT
   PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())` —
   the migration tracker itself.
3. Read the applied version set; for each embedded migration whose
   version is not in the set, run its SQL plus the
   `schema_migrations` insert in a single transaction. Failure
   rolls back the migration *and* the tracker row, so the next Open
   retries the same migration.
4. Release the advisory lock.

The mechanism is forward-only, hand-rolled (no migration library),
and matches what a load balancer rolling-restart of N nodes against
the same Postgres needs: every restart is a no-op except the one
that introduces a new migration file.

Per-publish writes run inside a transaction that takes a per-channel
advisory lock (`pg_advisory_xact_lock(hashtext(channel))`), so
concurrent writers serialise per channel without contending across
channels. Each node generates its own seriesId at process start; the
serial format itself (the `@seriesId` suffix) disambiguates concurrent
mints, so generator state is not shared across nodes.

Retention is a periodic background job that deletes the oldest log rows
on each channel using the channelSerial range — the first 14 characters
are the zero-padded mint timestamp in ms, so lex comparison matches
numeric comparison:

```sql
DELETE FROM channel_messages
WHERE channel = $1
  AND channel_serial < lpad(((now_ms - ttl_ms))::text, 14, '0');
```

A per-channel cap (counted in ChannelMessages = `DISTINCT
channel_serial`) is applied in the same sweep.

The intended default message TTL is **2 minutes**, matching Ably cloud's
default. The TTL value, the per-channel message cap, and whether operators
can override either are still being decided (TASK-26; see the retention
note at the top of §6).

## 7. Pub/Sub

Pub/sub turns a *publish* (originating from any node, via WS or REST)
into entries appended to the **local** Channel's linked list (§5.1)
on every node that has attachments to that channel. The flow is
unified across deployment modes: publish-path callers call
`channel.Publish(ctx, msgs)`, the storage backend persists, and the
Appender callback registered against each ChannelStore delivers the
committed cm to `channel.Append(cm)`. The Appender is the only
writer to the linked list in every mode.

Presence enter/update/leave ride this exact path: a presence operation
is a cm like any other (carrying `Presence` rather than `Messages`) and
reaches subscribers through the identical `storage → Append` mechanism,
including the cross-node NOTIFY round-trip in cluster mode (§12.2).

### 7.1 Single-process modes (`memory`, `disk`)

The Appender fires synchronously, inside the storage's `Store` call,
right after the persist commits. A publish is:

1. Authorise.
2. `channel.Publish(ctx, messages)` → `store.Store(ctx, messages)` —
   atomically:
   - checks any contained `Message.id` against this channel's
     idempotency index; on hit, returns the originally-persisted
     ChannelMessage with `idempotent=true` (no new row, no Appender
     call — the original was delivered when first persisted);
   - otherwise mints a fresh `channelSerial` (§8), stamps each
     `Message.serial = channelSerial + ":" + idx`, persists, and
     calls `appender.Append(cm)` — which is `core.Channel.Append`,
     linking the cm onto the live list.

Local subscribers parked on the previous tail's `notify` wake up and
observe the new entry. ACK/201 fires once `Publish` returns; the
linked-list update has already happened by then.

### 7.2 Cluster mode (Postgres broker)

Same `channel.Publish` API; the Appender fires from a dedicated
LISTEN goroutine running inside `postgres.Storage`. A publish is:

1. The publishing node's `store.Store(ctx, msgs)` mints the serial,
   `INSERT`s the rows, and emits
   `pg_notify('ably_channel', '{"channel":"...","serial":"..."}')`
   inside the same transaction. PG buffers the NOTIFY until commit,
   so listeners only see it if the publish committed.
2. **Every** node's LISTEN goroutine — including the publisher's
   — receives the NOTIFY, parses the JSON payload, looks up the
   local `ChannelStore` for that channel name, fetches the canonical
   cm by `(channel, channel_serial)`, and calls
   `appender.Append(cm)`. The publisher's local subscribers see the
   publish via this same round-trip — there is no fast-path direct
   Append; no self-vs-foreign dedup.

This means the publisher's local-visibility latency is one NOTIFY
round-trip (typically ~1–5ms against a same-region Postgres). The
trade-off is a single, symmetric delivery path: any cm reaches its
Channel via exactly one mechanism (the Appender callback), regardless
of which node minted it.

Notifications for channels that have never been opened on this node
(no `Channel(name, appender)` call yet) are silently dropped. The
canonical cm remains in storage and is picked up by the eventual
`ATTACH` via the history-replay path (§4.3).

The serial's format is itself the global ordering: the `<seriesId>`
suffix disambiguates serials minted in the same millisecond by
different processes, so storage's `ORDER BY channel_serial` reflects
a single global publish order without a central sequence. Local
linked-list arrival order on a given node approximates this but may
have small inversions under cross-node interleavings — canonical
order is the storage scan.

NOTIFY's 8KB payload limit is why we send pointers `(channel,
serial)` rather than full payloads. PG delivers notifications
at-most-once: a NOTIFY emitted while the LISTEN conn is down is not
redelivered when it reconnects. The broker therefore survives dropped
LISTEN connections rather than treating a `WaitForNotification` error
as fatal. When the conn drops, the LISTEN goroutine re-dials a fresh
`pgx.Conn` with capped exponential backoff, re-`LISTEN`s, and — before
resuming the notification loop — **reconciles** each registered
channel: it reads `History(AfterChannelSerial: lastSeen)` (both the
message and presence streams, merged in serial order) and delivers each
missed cm. Re-`LISTEN` precedes the reconcile scan, so any cm committed
during reconciliation is also buffered as a NOTIFY and observed once the
loop resumes — never lost in a gap between the snapshot and resubscribe.
It retries until the storage is `Close()`d.

Delivery is idempotent across the two paths. Every append — steady-state
NOTIFY dispatch and reconcile replay alike — funnels through a single
per-channel delivery point that tracks the highest `channel_serial`
handed to the appender and drops any cm whose serial is not strictly
greater. So a cm that arrives via both the reconnect history replay and
a subsequently-buffered NOTIFY reaches the Channel exactly once, in
order.

LISTEN/NOTIFY's well-known throughput ceiling is not a concern here:
ably-server targets developer-loop, CI, and modest single-region
self-host deployments. Operators who need cloud-scale throughput
should use Ably or fork.

## 8. Identifiers & ordering

- **connectionId**: 12-char base64 of random 9 bytes, generated on `CONNECTED`.
  Process-local; never persisted, never recoverable.
- **connectionKey**: the opaque key carried in `CONNECTED`'s
  `connectionDetails.connectionKey` for an SDK to resume with. Since
  connection-state resume is a non-goal (§1, §11), it is the
  `connectionId` — non-recoverable, and clients that present it on
  reconnect simply start a fresh connection.
- **clientId**: optional, resolved at auth time per §3.2. The server stamps
  it onto every outbound `Message.clientId` published by this connection,
  and rejects inbound frames that try to set a different value.
- **msgSerial** (per-connection publish counter): `*int64` on
  `ProtocolMessage`, assigned by the client; the server echoes it on
  `ACK`/`NACK` so the SDK can address publish acknowledgements. Every
  `ACK`/`NACK` carries an explicit `msgSerial`, including `0` for the
  first publish on a connection — SDKs correlate acknowledgements
  positionally and treat an absent serial as invalid — while frames the
  server never stamps (`HEARTBEAT`, `CONNECTED`, `MESSAGE` deliveries)
  omit the field; the pointer distinguishes "publish serial 0" from
  "no serial". An inbound frame's serial is read as absent-means-0.
  Distinct from `channelSerial` below — this is the wire field for
  publish flow control, not the canonical message ordering identifier.
- **ChannelMessage**: the atomic unit of a publish — one inbound
  REST request, or one `MESSAGE` frame carrying `messages[]`, lands
  on a channel as exactly one ChannelMessage containing one or more
  contained Messages. It is also the unit subscribers observe on the
  wire (one outbound `MESSAGE` frame per ChannelMessage) and the
  unit storage persists. A presence publish is the same unit carrying
  `Presence []*PresenceMessage` instead of `Messages` (§12.1), observed
  on the wire as one `PRESENCE` frame.
- **ChannelMessage.id** (message-publish batch id): the idempotency key
  for a message publish. When the publisher supplies no message ids, the
  server generates a random 8-character base64 batch id; when the
  publisher supplies ids, the batch id is derived from them. Either way
  the server stamps each contained `Message.id = "<batchID>:<idx>"`
  (idx unpadded, so a single-message publish stamps `"<batchID>:0"`),
  and the batch id is what storage indexes for idempotency (below). A
  client that supplies ids on a multi-message publish must make them
  conform to `"<batchID>:<idx>"`; a mismatch is rejected (`NACK` on WS,
  `400` on REST). A single-message publish accepts any client id (the
  batch id is that id with a trailing `:0` trimmed). The REST publish
  response's `messageId` is this stamped first-message id (§2.2).
- **PresenceMessage**: a single presence operation within a presence
  ChannelMessage — the presence-stream analogue of Message. It carries
  an `action` (ENTER/UPDATE/LEAVE inbound; PRESENT in sync; LEAVE/ABSENT
  outbound), a `clientId`, a server-stamped `connectionId`, and optional
  `data`. Like Message it splits `id` (optional, client-supplied
  idempotency key) from `serial` (server-assigned, `<channelSerial>:<idx>`).
  A member's key in the presence set is `connectionId:clientId`.
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
- **Message.serial**: the server-assigned **identity** of an individual
  message, of the form

  ```
  <channelSerial>:<idx>
  ```

  where `idx` is a zero-padded 3-digit position within the
  ChannelMessage (`000` for a single-message publish). All messages in a
  batch share the channelSerial prefix and differ only by `idx`. For a
  plain publish (`action: create`) the serial is the position of that
  publish. Crucially, it is **stable across versions**: when the message
  is later updated, deleted, or appended (§13), every version carries the
  *same* `serial` as the original create — the serial names the message,
  not the version.
- **Message.version**: present once mutable messages are in play (§13.1),
  the server-assigned identity of a *single version* of a message. It
  has the same `<channelSerial>:<idx>` shape — the position of the
  publish that produced this version — so a create has `version == serial`
  and each subsequent update/delete/append gets a fresh, strictly-greater
  `version`. Lexicographic comparison of versions gives newest-wins
  ordering. The wire `version` object also carries operation metadata
  (timestamp, the operating `clientId`, an optional description, and
  optional metadata).
- **Message.id**: the per-message identifier used for idempotent
  publishing, always of the form `"<batchID>:<idx>"` where `batchID` is
  the containing ChannelMessage's batch id (above). The server stamps it
  on every publish — deriving `batchID` from client-supplied ids, or
  generating one when none is supplied. The server enforces uniqueness
  per channel within the message retention window: because every message
  in a batch shares the same `batchID`, a repeat of a client-idempotent
  publish collides on its first message id and returns the original
  publish's `channelSerial` without re-appending. A publish whose batch
  id was server-generated carries a fresh random `batchID` each time, so
  it is always treated as new. `batchID` is opaque to the server —
  clients typically use a UUID or a deterministic hash of payload +
  intent.
- **Message.extras**: an optional free-form JSON object the client
  attaches to a message — headers, push metadata, and the AI Transport
  SDK's `extras.ai`. The server treats it as opaque and preserves it
  verbatim through publish, fan-out, storage, history and REST reads; a
  mutation carries the target's extras forward unless it supplies its own,
  which replaces the whole object (shallow-mixin, §13.2). `PresenceMessage`
  and `Annotation` carry the same `extras` field with identical semantics.
- **Message.timestamp**: the message's **create time** (server wall-clock ms,
  derived from the create serial), stamped at create and carried forward
  **unchanged** onto every later version — update, delete, append aggregate,
  and append delta alike. It is the message-identity timestamp, distinct from
  `version.timestamp`, which carries the *operation* time of the version that
  produced it. This mirrors the reference's `buildUpdateMessage`, whose
  top-level `timestamp` is the original message's timestamp. The field is
  `omitempty`: an unstamped (zero) value is dropped on the wire, so every
  delivery must carry it (an SDK reads the top-level timestamp as the create
  time and drives its retention/reorder clock from it).

Replay on `ATTACH` is a bounded history read from storage between the
client-supplied `channelSerial` and the channel's current head, streamed
after the `ATTACHED` ack. If the requested serial is older than retained
history the server attaches at the live head, clears
`ATTACHED.flags.RESUMED`, and populates `ATTACHED.error` so the SDK can
surface the discontinuity.

## 9. Configuration

CLI flags (each with an `ABLY_SERVER_*` env var equivalent, named by
upper-casing and underscoring the flag — e.g. `--log-format` is
`ABLY_SERVER_LOG_FORMAT`):

```
--mode {memory|disk|cluster}  default: memory
--listen :8080                HTTP/WS bind
--api-key                     appId.keyId:keySecret (repeatable; ABLY_SERVER_API_KEY is comma-separated)
--data-dir ./data             disk mode only
--db-dsn  postgres://…        cluster mode only
--shutdown-grace 10s          window to disconnect existing connections on SIGTERM
--log-level info
--log-format {text|json}
--debug-listen                pprof on a separate port; disabled if unset
--config ably-server.toml     optional TOML file, see below
--addr-file                   path to write the bound listener address to once listening
```

`--addr-file` writes the listener's resolved `host:port` to the given path
once the bind succeeds, then keeps running. It exists so a parent process
that started the server on `--listen 127.0.0.1:0` can discover the
ephemeral port the OS assigned — the sandbox provisioner (§15) relies on
it. The write is atomic (a sibling temp file renamed into place), so a
reader polling the path never sees a partial address.

Configuration may also be supplied via an optional TOML config file
(`--config ably-server.toml`), covering the same keys as the flags above
(`mode`, `listen`, `api-key`, `data-dir`, `db-dsn`, `shutdown-grace`,
`log-level`, `log-format`, `debug-listen` — `shutdown-grace` as a
duration string, e.g. `"10s"`). API keys may also be given as an
`api-keys` array; the file's `api-keys` and singular `api-key` are
combined. Keys may additionally be declared as structured `[[keys]]`
entries, each a `key` spec plus an optional `capability` — an
`x-ably-capability`-format JSON object string (§3.1) that scopes what the
key grants; omitting it grants the full capability, like a flag/env or
bare `api-key` entry. Within the file tier the `api-key`, `api-keys`, and
`[[keys]]` sources are all combined; only `[[keys]]` entries can carry a
narrowing capability. A malformed capability string is a startup error.

```toml
[[keys]]
key = "app.subscriber:s3cr3t"
capability = '{"chat:*":["subscribe"]}'
```

Every key is optional. Resolution order,
highest priority first: flag > env > config file > hardcoded default —
so a flag always wins, an env var beats the file, and the file only
supplies a value nothing more specific set.

The config file additionally carries the startup fixtures — everything
the server boots with is visible in one file, structured like the Ably
*test-app-setup* `post_apps` shape (the sandbox provisioner translates
that JSON into this config rather than the server parsing it):

- `[[namespaces]]` — a namespace `id` plus the `persisted`,
  `mutableMessages`, and `pushEnabled` feature flags. These are **parsed
  and recorded but behaviourally inert**: no behaviour keys off them yet;
  they exist so a provisioner can round-trip the full app shape. A
  namespace with no `id` is a startup error.
- `[[channels]]` — a channel `name` plus nested `[[channels.presence]]`
  member entries (`clientId`, `data`, `encoding`). At startup, before the
  listener opens, each member is entered through the normal
  `StorePresence` path so it lands in both the membership set and
  presence history, with a server-synthesized `connectionId`;
  `clientId`/`data`/`encoding` round-trip verbatim (encoding is opaque —
  cipher payloads are never decoded). Seeded members are static (§12.5).
  A channel with no `name` or a member with no `clientId` is a startup
  error. This exists purely so the ably-go presence suite, which the
  cloud sandbox provisions these members for, can run against a local
  server.

```toml
[[namespaces]]
id = "persisted"
persisted = true

[[channels]]
name = "persisted:presence_fixtures"

  [[channels.presence]]
  clientId = "client_string"
  data = "This is a string clientData payload"
```

## 10. Observability

- **Logs**: structured (`slog`), `text` for dev, `json` for prod.
- **Metrics**: Prometheus at `/metrics`, served unauthenticated on the main
  listener alongside `/healthz`. The series are process-wide and
  low-cardinality — no per-channel, per-connection, or per-clientId labels:
  - `ably_connections_opened_total` (counter) — WebSocket upgrades.
  - `ably_connections_open` (gauge) — currently-open WebSocket connections.
  - `ably_connection_lifetime_seconds` (histogram) — connection lifetime,
    upgrade to teardown.
  - `ably_attachments_total` (counter) — channel attachments established.
  - `ably_messages_published_total` (counter) — inbound publishes accepted
    (WebSocket + REST).
  - `ably_messages_delivered_total` (counter) — outbound `MESSAGE` frames
    forwarded to attachments.
  - `ably_publish_latency_seconds` (histogram) — inbound publish to
    storage-commit/ACK.
  - `ably_http_requests_total{route,method,status}` (counter) — REST requests
    by matched route pattern, method, and response status.

  Standard Go runtime and process collectors are also registered.
- **Tracing**: OpenTelemetry, off by default and configured entirely through
  the standard `OTEL_*` environment variables (`OTEL_EXPORTER_OTLP_ENDPOINT`,
  `OTEL_SERVICE_NAME`, `OTEL_TRACES_EXPORTER`, …). With no `OTEL_*` present no
  exporter is built and no goroutine is started — the no-op tracer carries no
  export overhead. When enabled, spans are exported over OTLP/HTTP and cover
  the WebSocket connection lifecycle (`ws.connection`), the publish path
  (`publish` / `channel.publish`), and — via otelhttp — REST request handling.
- **pprof**: behind `--debug-listen` on a separate port.

## 11. Lifecycle & operations

**Startup.** Each backend bootstraps its storage at `Open` time. The
bbolt backend creates the two top-level buckets if missing (§6.2).
The Postgres backend runs the auto-migrate sweep described in §6.3 —
a session-scoped advisory lock serialises N concurrently-starting
nodes so only one applies migrations, the rest observe the
`schema_migrations` tracker and skip.

On SIGTERM the server enters a graceful shutdown:

1. Stop accepting new WebSocket and HTTP connections.
2. Walk the registry of live WebSocket connections and, for each, send a
   `DISCONNECTED` frame (so SDKs reconnect elsewhere) and then close the
   socket — which drives the connection's normal teardown, including the
   synthesised presence `LEAVE`s (§12.5). These closures are **paced
   evenly across the `--shutdown-grace` window** (default `10s`) rather
   than fired all at once, so reconnects arrive at the next node
   staggered rather than as a thundering herd. Any connection still open
   at the deadline is force-closed immediately.
3. Concurrently, drain in-flight REST handlers.
4. Close storage.

In `cluster` mode each node is fungible. Rolling restart works because
clients are told to reconnect; the next node accepts the new connection
and, on each `ATTACH`, replays missed messages from Postgres using the
client-supplied `channelSerial`. No connection state crosses nodes.

## 12. Presence

Presence lets clients announce themselves as **members** of a channel —
each identified by a `clientId` — and observe other members entering,
updating their state, and leaving. It is modelled as a second kind of
message riding the channel's existing stream, plus a derived
**membership set** the server materialises so a late-arriving subscriber
can be brought up to date without replaying the whole stream.

### 12.1 Model

A presence operation is a publish like any other: it lands on the
channel as one ChannelMessage and flows through the unified
`storage → Appender.Append` path (§7). The only difference is the
payload — a ChannelMessage carries exactly one of `Messages` (a data
publish), `Presence` (a presence publish), or `Annotations` (an
annotation publish, §14):

```go
type PresenceMessage struct {
    ID           string         // client-supplied, optional, idempotency (§8)
    Serial       string         // server-assigned, "<channelSerial>:<idx>" (§8)
    Action       PresenceAction // ENTER | LEAVE | UPDATE | PRESENT | ABSENT
    ClientID     string
    ConnectionID string
    Data         any
    Encoding     string
    Timestamp    int64
}
```

`ID` and `Serial` carry the same split as on `Message` (§8): `ID` is the
optional client-supplied idempotency key, `Serial` is the server-assigned
`<channelSerial>:<idx>`.

`PresenceAction` matches Ably's wire enum: `ABSENT (0)`, `PRESENT (1)`,
`ENTER (2)`, `LEAVE (3)`, `UPDATE (4)`. A member's identity — its key in
the set — is `connectionId:clientId`, so the same `clientId` present
over two connections is two distinct members.

Messages and presence share **one ordered stream and one channelSerial
namespace**, so a single live list, a single Appender, and a single
NOTIFY path serve both. channelSerials are sortable cursors, not a dense
sequence, so the presence cms interleaved among data cms simply occupy
their own serials; a message-history scan skips them and a
presence-history scan skips data cms (the `kind` selector on
`storage.History`, §6). On the live linked list a subscriber's cursor
walks every cm and forwards each per its mode flags (§4.2): a
`SUBSCRIBE`-only attachment emits `MESSAGE` frames and ignores presence
cms; a `PRESENCE_SUBSCRIBE`-only attachment does the reverse.

The **membership set** is the fold of the presence stream: ENTER/UPDATE
establish or refresh a member (latest data wins), LEAVE removes it. The
server materialises this set (§12.5) so it can answer sync and
`GET .../presence` directly rather than re-deriving it from history on
every read.

### 12.2 Publishing presence (enter / update / leave)

An inbound `PRESENCE` frame carries `presence[]` of PresenceMessages with
action ENTER, UPDATE, or LEAVE, plus a `msgSerial` for flow control. The
connection:

1. Authorises — the attachment must hold the `PRESENCE` mode flag (which
   required the `presence` capability at attach time, §3.1); otherwise
   `NACK`.
2. Resolves and validates `clientId` (§12.3); a bad clientId → `NACK`.
3. Stamps `connectionId` and calls `channel.StorePresence(ctx, presence)`,
   which mints the channelSerial, stamps each `PresenceMessage.serial`,
   persists the presence cm onto the stream, folds it into the
   membership set, and — via the Appender — links it onto the live list.
   A client-supplied `PresenceMessage.id` is honoured for idempotency on
   the same per-channel index as message `id`s (§6).
4. Replies `ACK` / `NACK` on the `msgSerial`, exactly as for a data
   publish (§5.2).

Subscribers with `PRESENCE_SUBSCRIBE` observe the event as an outbound
`PRESENCE` frame delivered by their attachment cursor, identically to
how `MESSAGE` frames are delivered (§4.4).

### 12.3 Client identity

A presence member must be identified, so presence requires a concrete
`clientId`. The rules extend §3.2:

- The PresenceMessage's `clientId` must equal the connection's resolved
  `clientId`. A connection with a concrete resolved clientId may omit it
  on the frame (the server stamps its own); supplying a *different* one
  → `NACK`.
- A connection whose token asserts the `*` (wildcard) clientId may enter
  any concrete clientId but must supply one — `*` is never itself a
  member identity (§3.2).
- A connection with no resolved clientId (anonymous) cannot enter
  presence → `NACK` (`code: 91000`).

`connectionId` is always stamped by the server and cannot be set by the
client.

### 12.4 Sync

When a client attaches with `PRESENCE_SUBSCRIBE` to a channel whose
membership set is non-empty, the server sets the `HAS_PRESENCE` flag on
`ATTACHED` and then delivers the current set as one or more `SYNC`
frames before resuming live delivery:

```
client                         server
  │ ── ATTACH(flags incl. PRESENCE_SUBSCRIBE) ──▶
  │   ◀── ATTACHED(flags incl. HAS_PRESENCE) ───│
  │   ◀── SYNC(presence[], channelSerial="<s>:<cursor>") ─│  × pages
  │   ◀── SYNC(presence[], channelSerial="<s>:") ────────│  final (empty cursor)
  │   ◀── PRESENCE … (live) ─────────────────────│
```

Each `SYNC` frame carries a page of members as PresenceMessages with
action `PRESENT`. The `channelSerial` field doubles as the sync cursor:
`<serial>:<cursor>` while pages follow, `<serial>:` (empty cursor part)
on the final page to mark completion. At the scale we target the set
usually fits a single frame; the cursor protocol allows paging for larger
sets.

Consistency between the snapshot and live delivery is resolved by the
**client's merge**, exactly as in Ably: every PresenceMessage carries a
serial-based `serial` and the SDK keeps the newest per member key. A member
that enters or leaves in the window between the snapshot's as-of serial
and the live attach point arrives again on the cursor — a duplicate
ENTER is idempotent and a later LEAVE supersedes a stale PRESENT — so no
server-side coordination beyond taking the snapshot at-or-after the
attach point is required.

### 12.5 Membership set & liveness

The membership set is owned by the **storage backend**, alongside serial
state and the idempotency index — `StorePresence` folds each operation
into it transactionally and `Members` reads it (§6). Per backend:

- **memory** — a `map[memberKey]*PresenceMessage` per channel.
- **disk (bbolt)** — held in memory, *not* persisted. Presence is
  connection-scoped and no connection survives a process restart (§4.3),
  so the set is correctly empty on boot. Presence *history* still
  persists, as ordinary cms on the messages stream.
- **cluster (Postgres)** — the `presence` table (§6.3), upserted/deleted
  in the same transaction as the stream insert so the set is globally
  authoritative across nodes. A node serves sync and `GET .../presence`
  straight from `Members` (a `SELECT` against this table); it need never
  have witnessed the original ENTERs.

**Static fixture members.** Members seeded from the config file's
`[[channels]]` presence entries (§9) are the one exception to
connection-scoped liveness: they belong to no
connection, so no teardown ever synthesises a LEAVE for them, and in
cluster mode they are stored with a sentinel owner and a non-expiring
(`'infinity'`) lease so neither the lease-bump loop nor the reaper ever
touches them. They persist for the process's lifetime. This exists only
for SDK test-suite compatibility.

**Liveness.** A member lives exactly as long as the connection that
entered it. There is no presence grace period — consistent with §4.3 the
server holds no per-connection state across disconnects, so there is no
connection-resume window to keep a member alive for. (SDKs re-enter
presence on reconnect; the server treats that as a fresh ENTER under the
new `connectionId`.) Departure:

- **Explicit LEAVE, DETACH, or CLOSE** — processed as a LEAVE publish.
- **Connection drop (read error, heartbeat timeout) and graceful
  shutdown (§11)** — when the connection loop exits it synthesises a
  LEAVE for every member it entered (it tracks its own
  `(channel, clientId)` entries) and publishes them through
  `StorePresence`, so the departures persist, fold out of the set, and
  reach every subscriber on every node via the normal NOTIFY path.

**Crashed cluster nodes.** A node that dies without running teardown
leaves orphaned rows in the `presence` table — the one case the LEAVE
path cannot cover, and a cluster-only one (a single-process crash takes
the whole set down with it). Each `presence` row therefore records its
owning `node_id` and an `expires_at` lease, stamped by `StorePresence`
on ENTER/UPDATE. The storage backend runs two background loops on fixed
cadences (constants; operator config is a follow-up): a **lease-bump**
loop refreshes `expires_at` for every row the node owns in one
`UPDATE … WHERE node_id = $node`, at an interval comfortably shorter
than the lease window, so a live node's members never lapse; and a
**reaper** loop runs `DELETE FROM presence WHERE expires_at < now()
RETURNING …` — Postgres row locking means exactly one node's `RETURNING`
yields a given row, and that node synthesises the LEAVE for it through
the normal publish path (a fresh presence publish → NOTIFY → every
node's appender). This bounds orphan visibility to one lease window.

### 12.6 REST

- `GET /channels/{channel}/presence` — the current membership set,
  served from `Members`; requires `subscribe`. Returns a PresenceMessage
  array (each with action `PRESENT`). Accepts `clientId` and
  `connectionId` exact-match filters (RSP3a2/RSP3a3) and paginates with
  `limit` and the same `Link` convention as message history (§2.2), the
  member `serial` doubling as the opaque page cursor.
- `GET /channels/{channel}/presence/history` — presence history, a
  `kind=presence` history scan (§12.1) paginated with the same `Link`
  convention as message history (§2.2); requires `history`.

There is no REST *write* surface for presence: a member is inherently
bound to a realtime connection, so entering presence is realtime-only.

## 13. Mutable messages

Messages on a channel can be **updated**, **deleted**, and **appended**
to after they are published. Like presence (§12), this is modelled as
operations on the append-only stream rather than mutation of stored
state: a mutation is a fresh publish — a new ChannelMessage that
references a prior message's `serial` and carries an `action` — plus a
derived **latest-version** view the server materialises so reads and
history can resolve the current state of each message without replaying
its whole version chain.

### 13.1 Model — identity, version, action

Every Message carries an `action` and a stable `serial` (§8):

| `action` | meaning |
|---|---|
| `create` | an original publish (the default; what §2.1 / §7 already describe) |
| `update` | replace fields of an existing message with a new version |
| `delete` | soft-delete an existing message (a tombstone version) |
| `append` | concatenate onto an existing message's data (§13.3) |

`serial` names the **message**; `version` names a **single version** of
it (§8). A create mints both equal to its own `<channelSerial>:<idx>`.
An update/delete/append is an ordinary publish that lands at a *new*
channelSerial: its Message repeats the target's `serial`, sets the
`action`, and gets a fresh `version` (its own position). Versions sort
lexicographically, so newest-wins is a string comparison.

The action enum mirrors Ably's `MessageAction`, pinned to ably-go's
constants at implementation (`create = 0`, `update = 1`, `delete = 2`,
`append = 5`; `summary` / `meta` occupy other values). The wire shape
matches Ably: `serial`, `action`, and a `version` object
(`{serial, timestamp, clientId, description, metadata}`).

Mutations ride the **same stream and Append/NOTIFY path** as any publish
(§7): they are `kind = message` cms distinguished only by `action`
(presence stays `kind = presence`, §12.1). Nothing on the live linked
list or in storage is rewritten in place — a mutation is purely
additive, and the `serial`-vs-`version` split is what lets readers
collapse the chain.

### 13.2 Update & delete

An update or delete is published — REST `PATCH .../messages/{serial}`
(target serial in the path, `action` in the body), or a WS `MESSAGE`
frame carrying the `action` and the target `serial`. The server:

1. Authorises against `message-{update,delete}-{own,any}` (§3.1): `-own`
   requires the caller's resolved `clientId` to equal the target
   message's creator; `-any` waives it. The WS surface also still
   requires the `PUBLISH` attachment mode.
2. Resolves the target's current latest version from the latest-version
   fold (§13.4). A target that does not exist (never published, or aged
   out of retention) is rejected (`ERROR` / `NACK` on WS, `4xx` on REST).
3. Applies **shallow-mixin** semantics: only the fields supplied among
   `data`, `name`, `extras` replace the corresponding fields of the
   current version; unspecified fields are carried forward. The server
   persists the resulting **merged** Message as the new version, so
   subscribers and history always carry a complete message, never a diff.
4. Stamps operation metadata into `version` (timestamp, operating
   `clientId`, optional description/metadata), mints the new `version`,
   persists the cm on the stream, and updates the `serial → versions`
   index and the latest-version fold (§13.4). The **top-level
   `Message.timestamp` is left as the original create time** (§8) — only
   `version.timestamp` carries this operation's time.
5. ACKs / NACKs (WS) or responds (REST) as for any publish.

A `delete` is **soft**: it writes a tombstone version (the latest fold
marks the message deleted) but the message and all its versions remain
queryable via version history (§13.4). Subscribers receive the new
version as an ordinary outbound `MESSAGE` frame carrying `action: update`
/ `delete` and the unchanged `serial` — delivered by the same attachment
cursor as any message (§4.4), so a live subscriber sees the edit or
removal in stream order.

### 13.3 Append

`append` concatenates `data` onto the message's current latest version
(name / extras follow the same shallow-mixin replace as update). It
targets the high-frequency single-publisher case (e.g. streaming an LLM
token sequence onto one message), so its delivery is looser than update /
delete:

- The server maintains the **rolled-up** latest data for the message. An
  append is persisted and fanned out as a full `action: update` whose
  `data` is the aggregate so far, carrying the incremental append in
  `alt["delta-append"]` (an `action: append` message holding the new
  `data`, sharing the version). The delta repeats the message's identity —
  its `name`, `extras` and top-level create `timestamp`, carried forward
  from the create (name / extras replaced by the append's own when it
  supplies them, §13.2) — so a subscriber routes an append frame by `name`
  and `extras` exactly as the create; only its `data` is the incremental
  slice. A streaming publisher omits the `name` on each append (relying on
  this carry-forward), so a name-less delta would be invisible to a
  name-filtering subscriber. The delivery path chooses per subscriber:
  a subscriber that is caught up receives the delta **incrementally**
  (`action: append`, just the new `data`); the first delivery for a
  message a subscriber has not yet seen — e.g. immediately after attach,
  or backlog replay on resume/rewind — is the full `action: update`
  carrying the aggregate, after which it receives subsequent appends
  incrementally. This needs per-attachment tracking of which message
  identities the subscriber has seen since attach.
- The server may **conflate**: coalesce multiple appends, drop superseded
  intermediate versions, or deliver an append as a full rolled-up
  `update`. The only guarantee is that the last version a subscriber
  receives is the most recent — there is no promise that every
  intermediate append is delivered. Collapsing the pre-attach append run
  into a single full-on-first-sight update is exactly this: the
  intermediate appends a fresh subscriber never saw are never sent.
- Appends are **not** retained as individual entries in version history;
  only the aggregated latest version is durable (§13.4). The
  `appendMode=full` channel param opts a subscriber out of incremental
  delivery, so it always receives the full rolled-up versions instead of
  append deltas.

Append aggregation is inherently stateful and conflation-sensitive, which
is why its delivery contract is deliberately looser than that of update /
delete.

### 13.4 Reads & history

Two derived structures, both owned by the storage backend (§6) and
maintained transactionally with each version's persist into the
`channel_messages` log — the message analogues of the presence membership
set:

- **the materialised `messages` projection** (`serial → latest merged
  Message`, with a deleted tombstone). Backs `GET .../messages/{serial}`
  (one message, latest version) and the default `GET .../messages`
  history, which returns the **latest version of each message positioned
  at its create serial** — an edited message keeps its place in the
  timeline but shows current content, and a deleted message shows as a
  tombstone.
- **the `serial → versions` index** over the log. Backs
  `GET .../messages/{serial}/versions`, which returns the versions of a
  message ordered by `version` — oldest-first (create then edits) by
  default, since a version chain reads naturally forwards and the SDK
  requests it without a direction; an explicit `direction` param still
  wins. Paginated with the same `Link` convention as message history
  (§2.2). It is also what an update / delete consults
  to validate and merge against its target. Appends are the exception to
  "every version": the log keeps each append cm for live and resume
  fan-out, but the versions read-path collapses a run of appends to its
  aggregate, so a streamed append shows as one evolving version rather
  than one entry per delta (§13.3).

Live and resume delivery are **not** collapsed: a fresh subscriber, and a
resuming one replaying the gap (§4.3), receive the raw version cms in
stream order (create, then each edit) and converge by newest-`version`
exactly as they would have live. Only history *reads* collapse to the
latest version. `rewind` (§4.3) counts stream cms, so a window may span
several versions of the same message.

### 13.5 Capabilities

Mutations add four capability ops to §3.1, matching Ably:
`message-update-own`, `message-update-any`, `message-delete-own`,
`message-delete-any` (append is gated by `message-update-*`). The `-own`
/ `-any` distinction is the first **ownership-scoped** op in the model:
`-own` resolves only if the caller's `clientId` (§3.2) equals the target
message's creator `clientId`; `-any` skips the check. Creating a message
is still plain `publish`; all version reads (latest, by-serial, versions)
use `history`.

### 13.6 Surface

- **WS** — inbound `MESSAGE` carries `action` + (for mutations) the
  target `serial`; outbound `MESSAGE` carries `action` + `version`. No
  new `ProtocolMessage` action is introduced — mutations reuse `MESSAGE`
  (15), distinguished by the Message-level `action` field.
- **REST** — `POST .../messages` creates; `PATCH .../messages/{serial}`
  carries a mutation (target serial in the path, `action` in the body);
  `GET .../messages/{serial}` and `GET .../messages/{serial}/versions`
  read the latest version and the full version chain (§2.2).

## 14. Annotations & summaries

An **annotation** is a piece of metadata a client attaches to an existing
message — a reaction, a citation, a flag — identified by a `type` and
aggregated by the server into a per-message **summary** that subscribers
and history readers consume instead of the raw annotation stream. As with
presence (§12) and mutations (§13), annotations are operations on the
append-only stream plus a derived view: nothing is rewritten in place.

### 14.1 Model

An annotation is a publish like any other: it lands on the channel as one
ChannelMessage carrying `Annotations []*protocol.Annotation` (never
`Messages` or `Presence`) and flows through the unified
`storage → Appender.Append` path (§7), as a third stream kind
(`kind = annotation`) sharing the channelSerial namespace.

An Annotation carries: `id` / `serial` with the same split as Message
(§8 — client-supplied idempotency key vs server-assigned
`<channelSerial>:<idx>`), an `action` (`annotation.create = 0`,
`annotation.delete = 1`), the target `messageSerial`, a `type`, optional
`name`, `count`, `data`/`encoding`, and the attributed `clientId` plus
server-stamped `connectionId` and `timestamp` — field names and enum
values pinned to Ably's wire shape.

The `type` has the form `<name>:<aggregation>` where `<aggregation>` is
one of the five v1 **summarisation methods**: `distinct.v1` (per-value
set of distinct clientIds), `unique.v1` (one value per clientId, newest
wins), `multiple.v1` (per-value counts, `count` honoured), `flag.v1`
(boolean per clientId), `total.v1` (anonymous tally). Validation mirrors
Ably: a missing/malformed `messageSerial` or `type` is rejected (40000).

**Identity.** Annotation publishes are attributed: a concrete `clientId`
is required (resolved per §3.2), except that the `multiple.v1` and
`total.v1` methods also accept anonymous publishes — mirroring Ably's
anonymous-aggregation allowance. An `annotation.delete` removes the
caller's own contribution(s) for the type, per the method's fold.

**Target existence.** The target message must resolve in the
latest-version projection (§13.4) — i.e. exist within retention — or the
publish is rejected, exactly as for update/delete.

### 14.2 Summary fold

The summary is the fold of a message's annotations, computed **by the
storage backend transactionally at store time** — the same pattern as the
presence membership set (§12.5) and the latest-version projection
(§13.4). `StoreAnnotation` mints the channelSerial, persists the
annotation cm on the log, folds it into the target message's summary (a
`map[type]aggregation` keyed by the full `<name>:<aggregation>` type),
and stamps the **post-fold summary snapshot onto the stored cm** before
it reaches the Appender.

That snapshot is what makes cluster delivery deterministic: every node —
including ones that never witnessed earlier annotations — emits the
summary for a given annotation cm from the cm itself, off the normal
Append/NOTIFY path (§7.2). No node ever publishes a separate rollup, so
there is no duplicate-summary problem and no cross-node coordination
beyond the existing per-channel advisory lock. There is no debounce:
one summary delivery per annotation, which Ably's conflation latitude
permits (only the *latest* summary a subscriber holds matters).

The summary is stored on the messages projection row (the merged latest
Message carries its `summary`), so it dies with the message under
whatever retention policy applies (TASK-26) — a summary is message
state, not independent state.

### 14.3 Delivery

On Append of an annotation cm a node emits, per attachment mode (§4.2):

- **`ANNOTATION` (21)** — the raw annotation, only to attachments
  holding `ANNOTATION_SUBSCRIBE`. Publishing requires the
  `ANNOTATION_PUBLISH` mode on the attachment (WS) and is NACKed
  without it.
- **`MESSAGE` with `action: summary` (4)** — the post-fold summary
  snapshot, carrying the target message's unchanged `serial` and the
  `summary` object, to ordinary `SUBSCRIBE` attachments. This is how a
  subscriber that knows nothing about annotations sees reactions/
  citations accumulate. Wire shape:
  `{"action": 4, "serial": "<target>", "summary": {"<type>": {…}}}`
  with the per-method value shapes (`{value: {total, clientIds}}` for
  distinct/unique, counts for multiple, etc.) pinned to Ably's.

The two new mode flags extend the §4.2 table: `ANNOTATION_PUBLISH`
(`1 << 21`) and `ANNOTATION_SUBSCRIBE` (`1 << 22`), permitted iff the
capability grants `annotation-publish` / `annotation-subscribe`
respectively (§3.1). When `ATTACH.flags` carries no mode bits, the
default set does **not** include the annotation modes (matching SDK
defaults); clients opt in via modes or channel params.

Both frames are a **live** delivery derived from the one annotation cm as
the attachment cursor walks it: the raw `ANNOTATION` goes to
`ANNOTATION_SUBSCRIBE` attachments, and the summary `MESSAGE` — built from
the snapshot the cm carries, never recomputed — to `SUBSCRIBE` ones. The
summary is not a separate persisted cm; it exists only on the projection
row (for reads) and as the snapshot stamped on the annotation cm (for
delivery). The resume gap replay (§4.3) is a `kind = message` history
scan, so — exactly as it skips presence — it skips annotation cms
entirely; neither the raw annotation nor its derived summary is replayed
as a frame. A resuming subscriber re-reads raw annotations via the REST
endpoint (§14.4) and reconverges on the current summary through the
projection-backed message reads (§14.4) — the newest summary always rides
the latest version of its message — and via any subsequent live
annotation. History *reads* do not enumerate annotation cms: they return
messages whose embedded `summary` is current, and a `kind = message`
history scan skips annotation cms exactly as it skips presence.

### 14.4 Reads

- `GET /channels/{channel}/messages/{serial}/annotations` — the raw
  annotations for one message, in stream order, paginated with the
  standard `Link` convention (§2.2); requires `history` (reads) —
  publishes via `POST` on the same path require `annotation-publish`.
- Message reads (`GET .../messages`, `GET .../messages/{serial}`,
  history) carry the current `summary` on each message via the
  latest-version projection — no separate summary endpoint.

Storage: annotation log rows are `kind = annotation` with
`message_serial` holding the **target** message serial, so the existing
serial index serves annotations-for-message scans (message-version scans
add `kind = 'message'` to their predicates, §6.3). The summary lives in
the messages projection payload; bbolt mirrors this in its `messages`
bucket value, memory in its projection map.

### 14.5 Capabilities

Two ops extend §3.1, matching Ably: `annotation-publish` (publish an
annotation, WS or REST) and `annotation-subscribe` (receive raw
`ANNOTATION` frames). Summaries require only `subscribe` — they are
message deliveries. Reading annotations via REST requires `history`.

## 15. Sandbox provisioner

The `ably-server` binary is strictly single-app: one app's keys, one
channel namespace. Ably's SDK test suites, however, provision a fresh app
per test run against a sandbox REST host (`POST /apps`), then point clients
at the returned keys. The `ably-sandbox` command bridges the two — it is
the disposable-instance-per-test-run role from PDR-090. Its working name
rides TASK-75's final naming decision.

`ably-sandbox` is a separate process that serves the harness-facing
provisioning API (default `:9080`) and spawns **one `ably-server` child
per provisioned app**, so the core server never grows multi-app
machinery. It keeps the core single-app while giving the harness the
multi-app surface it expects.

**`POST /apps`** accepts the Ably *test-app-setup* `post_apps` body — keys
(each with an optional capability, carried either as a stringified JSON
object or a nested object; unrecognised fields such as `revocableTokens`
are echoed back untouched), `namespaces`, and `channels` with presence
members. For each request the provisioner:

- generates an `appId`/`accountId` and, per key, an `appId.keyId:keySecret`
  triple in the exact format the SDKs parse (the random tokens use a
  base64url alphabet, so they never contain `.` or `:`);
- translates the body into a temporary TOML config expressing the
  `[[keys]]` (with capabilities), `[[namespaces]]`, and
  `[[channels]]`/presence sections (§9), emitted through the same
  `internal/config` types the server parses, so escaping round-trips;
- boots a child `ably-server --config <tmp> --mode memory --listen
  127.0.0.1:0 --addr-file <tmp>`, discovers the bound port from the
  addr-file (§9), and
- responds `201` with the sandbox app JSON — `appId`, `accountId`, `keys[]`
  (each carrying `keyName`, `keySecret`, `keyStr`, and the capability as a
  JSON string), the echoed `namespaces`/`channels`/`cipher` blocks —
  **extended** with `endpoint`/`port`/`tls` fields so the harness can route
  clients straight at the child. The response preserves the harness
  invariant that `keys` and `namespaces` count-match the request.

**`DELETE /apps/{appId}`** terminates that child (SIGTERM to its process
group, escalating to SIGKILL after a grace window) and is idempotent
(`204` whether or not the app is known). **`POST /stats`** accepts and
discards stats fixtures (`201`), mirroring the server's stub (§1): the
harness posts them at the provisioning host.

Children run in their own process group so a single signal reaches the
real server even under the `go run` fallback, and inherit an environment
with `ABLY_SERVER_*` stripped so a variable set for the provisioner can't
override the per-app config. Each child's stdout/stderr and its config go
to a per-app directory under a temp dir. The provisioner tracks
`appId → process + port + last-touched`; an idle-TTL reaper (default 30m)
kills children not provisioned or deleted within the window, and a SIGTERM
to the provisioner tears down every child before it exits. Concurrent
`POST /apps` calls produce independent children on distinct OS-assigned
ports with no shared state.

The provisioner finds the `ably-server` binary via `--server-bin`; absent
that, a sibling named `ably-server` next to its own executable; absent
that, it falls back to `go run ./cmd/ably-server` (which assumes the
working directory is the module root, the case when the provisioner itself
runs under `go run`).

## 16. Testing strategy

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

## 17. Project layout & licensing

- License: **Apache 2.0** (matches ably-go).
- Module: `github.com/ably/ably-server`.
- Go version floor: **1.26**.
