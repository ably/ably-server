// Example Fastify host app embedding ably-server.
//
// Run:
//   ABLY_PUBLIC_PORT=8542 node examples/fastify-app.js
//   # subpath mount: ABLY_MOUNT=/ably ABLY_PUBLIC_PORT=8542 node examples/fastify-app.js
//
// Then point an unmodified ably-js SDK at 127.0.0.1:<ABLY_PUBLIC_PORT> (tls:false).
// Browser demo is served at /demo/.

import path from 'node:path';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import Fastify from 'fastify';
import { AblyServer } from '../src/index.js';
import { registerAblyProxy } from '../src/fastify.js';

const PORT = Number(process.env.ABLY_PUBLIC_PORT ?? 8542);
const MOUNT = process.env.ABLY_MOUNT ?? '/';
const DEMO_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', '..', 'demo');
const demoHtml = readFileSync(path.join(DEMO_DIR, 'index.html'), 'utf8');

const server = new AblyServer({
  apiKey: process.env.ABLY_SERVER_API_KEY ?? 'app.key:secret',
});
await server.start();
console.log(`[fastify-app] embedded ably-server ready on internal port ${server.port} (pid ${server.child.pid})`);
server.on('exit', ({ code, signal }) => console.log(`[fastify-app] embedded server exited (code=${code} signal=${signal})`));
server.on('restart', (n) => console.log(`[fastify-app] restarting embedded server (attempt ${n})`));
server.on('ready', (port) => console.log(`[fastify-app] embedded server (re)ready on internal port ${port} (pid ${server.child?.pid})`));

// forceCloseConnections drops lingering keep-alive/WS sockets on close() so
// a SIGTERM shutdown completes promptly instead of hanging on open sockets.
const fastify = Fastify({ forceCloseConnections: true });
// The host app's own routes (registered before the proxy so they win):
// the browser demo, and a home page when Ably is under a subpath.
fastify.get('/demo', (_req, reply) => reply.type('text/html').send(demoHtml));
fastify.get('/demo/', (_req, reply) => reply.type('text/html').send(demoHtml));
if (MOUNT !== '/') {
  fastify.get('/', (_req, reply) => reply.type('text/plain').send(`host app home — Ably is embedded under ${MOUNT}/`));
}
await registerAblyProxy(fastify, { server, mountPath: MOUNT });

await fastify.listen({ port: PORT, host: '0.0.0.0' });
console.log(`[fastify-app] listening on http://127.0.0.1:${PORT} — Ably under ${MOUNT}, demo at /demo/`);

// --- graceful shutdown: stop the app AND the child (no orphan) ---
let shuttingDown = false;
async function shutdown(sig) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[fastify-app] received ${sig}, shutting down...`);
  await fastify.close();
  await server.stop();
  console.log('[fastify-app] clean shutdown complete');
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
