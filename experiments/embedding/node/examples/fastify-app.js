// Example Fastify host app embedding ably-server.
//
// Run:
//   ABLY_PUBLIC_PORT=8542 node examples/fastify-app.js
//
// Then point an unmodified ably-js SDK at 127.0.0.1:<ABLY_PUBLIC_PORT> (tls:false).

import Fastify from 'fastify';
import { startEmbeddedServer } from '../src/index.js';
import { registerAblyProxy } from '../src/fastify.js';

const PORT = Number(process.env.ABLY_PUBLIC_PORT ?? 8542);

const supervisor = await startEmbeddedServer({
  apiKey: process.env.ABLY_SERVER_API_KEY ?? 'app.key:secret',
});
console.log(`[fastify-app] embedded ably-server ready on internal port ${supervisor.port} (pid ${supervisor.child.pid})`);
supervisor.on('exit', ({ code, signal }) => console.log(`[fastify-app] embedded server exited (code=${code} signal=${signal})`));
supervisor.on('restart', (n) => console.log(`[fastify-app] restarting embedded server (attempt ${n})`));
supervisor.on('ready', (port) => console.log(`[fastify-app] embedded server (re)ready on internal port ${port} (pid ${supervisor.child?.pid})`));

// forceCloseConnections drops lingering keep-alive/WS sockets on close() so
// a SIGTERM shutdown completes promptly instead of hanging on open sockets.
const fastify = Fastify({ forceCloseConnections: true });
await registerAblyProxy(fastify, { supervisor });

await fastify.listen({ port: PORT, host: '0.0.0.0' });
console.log(`[fastify-app] listening on http://127.0.0.1:${PORT} (proxying to embedded ably-server)`);

// --- graceful shutdown: stop the app AND the child (no orphan) ---
let shuttingDown = false;
async function shutdown(sig) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[fastify-app] received ${sig}, shutting down...`);
  await fastify.close();
  await supervisor.stop();
  console.log('[fastify-app] clean shutdown complete');
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
