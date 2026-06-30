# @ably/embedded-server (Node) — PoC

Embed an **unmodified** `ably-server` inside your Node host app: the package
spawns the prebuilt Go binary as a supervised child process on a free local
port, and a tiny middleware reverse-proxies **all** realtime (WebSocket) and
REST traffic to it on a **dedicated port**. Your existing `ably-js` SDK points
at that port — no SDK changes (EMBEDDING-POC.md §5, §6).

```
your Node app (public port 8541)
  ├─ http-proxy-middleware / @fastify/http-proxy  ──►  ably-server child
  └─ supervisor (free port, /readyz, restart-on-crash, stop-with-app)        (127.0.0.1:<free>)
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
import { startEmbeddedServer } from '@ably/embedded-server';
import { mountAblyProxy } from '@ably/embedded-server/express';

const supervisor = await startEmbeddedServer({ apiKey: 'app.key:secret' });

const app = express();
const proxy = mountAblyProxy(app, { supervisor });

const server = app.listen(8541);
server.on('upgrade', proxy.upgrade); // WebSocket upgrades bypass Express middleware

process.on('SIGTERM', async () => {
  server.closeAllConnections();
  await new Promise((r) => server.close(r));
  await supervisor.stop();
  process.exit(0);
});
```

That is the **entire** integration: ~9 lines of glue.

## Use it — Fastify

```js
import Fastify from 'fastify';
import { startEmbeddedServer } from '@ably/embedded-server';
import { registerAblyProxy } from '@ably/embedded-server/fastify';

const supervisor = await startEmbeddedServer({ apiKey: 'app.key:secret' });

const fastify = Fastify({ forceCloseConnections: true });
await registerAblyProxy(fastify, { supervisor });
await fastify.listen({ port: 8542, host: '0.0.0.0' });

process.on('SIGTERM', async () => {
  await fastify.close();
  await supervisor.stop();
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
| `startEmbeddedServer(opts)` | `@ably/embedded-server` | construct + start a supervisor; resolves once `/readyz` is green |
| `AblyServerSupervisor` | `@ably/embedded-server` | the supervisor class (events: `ready`, `spawn`, `exit`, `restart`, `error`) |
| `mountAblyProxy(app, { supervisor })` | `@ably/embedded-server/express` | returns `{ middleware, upgrade }`; wire `upgrade` to `server.on('upgrade', …)` |
| `registerAblyProxy(fastify, { supervisor })` | `@ably/embedded-server/fastify` | registers `@fastify/http-proxy` with `websocket: true` |

### `startEmbeddedServer` options

| Option | Default | Meaning |
|---|---|---|
| `apiKey` | `$ABLY_SERVER_API_KEY` or `app.key:secret` | API key `appId.keyId:keySecret` |
| `binaryPath` | `$ABLY_SERVER_BINARY` or `./bin/ably-server` | path to the prebuilt binary |
| `port` | OS-assigned free port | fixed internal port (omit for auto) |
| `mode` | `memory` | server storage mode |
| `logLevel` | `error` | child log level |
| `shutdownGrace` | `10s` | child graceful-shutdown window |
| `readyTimeoutMs` | `10000` | `/readyz` poll timeout |
| `maxRestarts` | `5` | consecutive crash-restarts before giving up |
| `onChildLog` | — | `(line) => void` to capture child stdout/stderr |

## What it does for you

- **Auto free port** — the supervisor binds `:0`, reads back the port, and
  passes `--listen 127.0.0.1:<port>` to the child.
- **Readiness gate** — `startEmbeddedServer()` resolves only after `/readyz`
  returns 200 (or rejects on timeout).
- **Restart on crash** — an unexpected child exit is respawned (capped,
  exponential backoff) on the **same** internal port, so the proxy upstream
  stays valid.
- **Stop with the app** — `supervisor.stop()` SIGTERMs the child and waits
  (SIGKILL on grace timeout); wiring it into your shutdown handler guarantees
  no orphaned process.
