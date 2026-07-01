# @ably/embedded-server (Node) — PoC

Embed an **unmodified** `ably-server` inside your Node host app: the package
spawns the prebuilt Go binary as a supervised child process on a free local
port, and a tiny middleware reverse-proxies **all** realtime (WebSocket) and
REST traffic to it on a **dedicated port**. Your existing `ably-js` SDK points
at that port — no SDK changes (EMBEDDING-POC.md §5, §6).

```
your Node app (public port 8541)
  ├─ http-proxy-middleware / @fastify/http-proxy  ──►  ably-server child
  └─ AblyServer (free port, /readyz, restart-on-crash, stop-with-app)        (127.0.0.1:<free>)
```

## Prerequisites

- Node ≥ 20.
- The prebuilt `ably-server` binary at `./bin/ably-server` (gitignored). Build
  it once from the repo root:

  ```sh
  go build -o experiments/embedding/node/bin/ably-server ./cmd/ably-server
  ```

  In a published package the binary would instead ship per-platform via npm
  `optionalDependencies` keyed by `os`/`cpu` (the esbuild precedent); for this
  PoC it is resolved from `./bin` or `$ABLY_SERVER_BINARY`.

## Install

```sh
cd experiments/embedding/node
npm install
```

## Use it — Express

```js
import express from 'express';
import { AblyServer } from '@ably/embedded-server';
import { mountAblyProxy } from '@ably/embedded-server/express';

const server = new AblyServer({ apiKey: 'app.key:secret' });
await server.start();                       // spawn the child, wait for /readyz

const app = express();
const proxy = mountAblyProxy(app, { server });

const httpServer = app.listen(8541);
httpServer.on('upgrade', proxy.upgrade); // WebSocket upgrades bypass Express middleware

process.on('SIGTERM', async () => {
  httpServer.closeAllConnections();
  await new Promise((r) => httpServer.close(r));
  await server.stop();                      // stop the child, no orphan
  process.exit(0);
});
```

That is the **entire** integration: ~9 lines of glue. (`await startEmbeddedServer({ … })`
is a one-line convenience for `new AblyServer(…)` + `start()` if you prefer.)

## Use it — Fastify

```js
import Fastify from 'fastify';
import { AblyServer } from '@ably/embedded-server';
import { registerAblyProxy } from '@ably/embedded-server/fastify';

const server = new AblyServer({ apiKey: 'app.key:secret' });
await server.start();

const fastify = Fastify({ forceCloseConnections: true });
await registerAblyProxy(fastify, { server });
await fastify.listen({ port: 8542, host: '0.0.0.0' });

process.on('SIGTERM', async () => {
  await fastify.close();
  await server.stop();
  process.exit(0);
});
```

## Connect with the unmodified ably-js SDK

```js
import * as Ably from 'ably';

const client = new Ably.Realtime({
  key: 'app.key:secret',
  restHost: '127.0.0.1',
  realtimeHost: '127.0.0.1',
  port: 8541,        // your app's public port
  tls: false,        // local PoC
});

const channel = client.channels.get('demo');
channel.subscribe('greeting', (m) => console.log('got', m.data));
await channel.attach();
await channel.publish('greeting', { hello: 'world' });
```

## Try the examples

```sh
# Express on :8541
ABLY_PUBLIC_PORT=8541 npm run example:express
# Fastify on :8542
ABLY_PUBLIC_PORT=8542 npm run example:fastify
# round-trip a message through the proxy with the real ably-js SDK
ABLY_PUBLIC_PORT=8541 npm run smoke
```

## API

| Export | From | Purpose |
|---|---|---|
| `AblyServer` | `@ably/embedded-server` | the embedded server object a host holds (an `EventEmitter`; events: `ready`, `spawn`, `exit`, `restart`, `error`). Construct with `new`, then `await server.start()` / `await server.stop()`. |
| `startEmbeddedServer(opts)` | `@ably/embedded-server` | one-line convenience: `new AblyServer(opts)` + `start()`; resolves once `/readyz` is green |
| `mountAblyProxy(app, { server })` | `@ably/embedded-server/express` | returns `{ middleware, upgrade }`; wire `upgrade` to `httpServer.on('upgrade', …)` |
| `registerAblyProxy(fastify, { server })` | `@ably/embedded-server/fastify` | registers `@fastify/http-proxy` with `websocket: true` |

`AblyServerSupervisor` is exported as a back-compat alias for `AblyServer`; the
proxy adapters also still accept `{ supervisor }`. New code should use
`AblyServer` / `{ server }`.

### `AblyServer` options (also accepted by `startEmbeddedServer`)

| Option | Default | Meaning |
|---|---|---|
| `apiKey` | `$ABLY_SERVER_API_KEY` or `app.key:secret` | API key `appId.keyId:keySecret` |
| `binaryPath` | `$ABLY_SERVER_BINARY` or `./bin/ably-server` | path to the prebuilt binary |
| `port` | OS-assigned free port | fixed internal port (omit for auto) |
| `mode` | `memory` | storage mode: `memory` (default), `disk` (needs `dataDir`), `cluster` (needs `dbDsn`) |
| `dataDir` | — | data directory for `disk` mode (passed as `--data-dir`) |
| `dbDsn` | — | Postgres DSN for `cluster` mode (passed as `--db-dsn`) |
| `logLevel` | `info` | child log level |
| `shutdownGrace` | `10s` | child graceful-shutdown window |
| `readyTimeoutMs` | `10000` | `/readyz` poll timeout |
| `maxRestarts` | `5` | consecutive crash-restarts before giving up |
| `onChildLog` | writes to `process.stderr` | `(line) => void` to capture child stdout/stderr; pass `() => {}` to mute |

## What it does for you

- **Auto free port** — the embedded server binds `:0`, reads back the port, and
  passes `--listen 127.0.0.1:<port>` to the child.
- **Readiness gate** — `server.start()` resolves only after `/readyz`
  returns 200 (or rejects on timeout).
- **Restart on crash** — an unexpected child exit is respawned (capped,
  exponential backoff) on the **same** internal port, so the proxy upstream
  stays valid.
- **Stop with the app** — `server.stop()` SIGTERMs the child and waits
  (SIGKILL on grace timeout); wiring it into your shutdown handler guarantees
  no orphaned process.
