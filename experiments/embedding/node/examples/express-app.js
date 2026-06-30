// Example Express host app embedding ably-server.
//
// This is the glue a host-app developer writes. Run:
//   ABLY_PUBLIC_PORT=8541 node examples/express-app.js
//
// Then point an unmodified ably-js SDK at 127.0.0.1:<ABLY_PUBLIC_PORT> (tls:false).

import express from 'express';
import { startEmbeddedServer } from '../src/index.js';
import { mountAblyProxy } from '../src/express.js';

const PORT = Number(process.env.ABLY_PUBLIC_PORT ?? 8541);

const supervisor = await startEmbeddedServer({
  apiKey: process.env.ABLY_SERVER_API_KEY ?? 'app.key:secret',
});
console.log(`[express-app] embedded ably-server ready on internal port ${supervisor.port} (pid ${supervisor.child.pid})`);
// Visibility into crash-recovery.
supervisor.on('exit', ({ code, signal }) => console.log(`[express-app] embedded server exited (code=${code} signal=${signal})`));
supervisor.on('restart', (n) => console.log(`[express-app] restarting embedded server (attempt ${n})`));
supervisor.on('ready', (port) => console.log(`[express-app] embedded server (re)ready on internal port ${port} (pid ${supervisor.child?.pid})`));

const app = express();
const proxy = mountAblyProxy(app, { supervisor });

const server = app.listen(PORT, () => {
  console.log(`[express-app] listening on http://127.0.0.1:${PORT} (proxying to embedded ably-server)`);
});
server.on('upgrade', proxy.upgrade); // WebSocket upgrades bypass the Express stack

// --- graceful shutdown: stop the app AND the child (no orphan) ---
let shuttingDown = false;
async function shutdown(sig) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[express-app] received ${sig}, shutting down...`);
  // Order matters. Stop the embedded child FIRST: that tears down the
  // proxied upstream connections, so http-proxy closes the corresponding
  // downstream client sockets and the HTTP server can actually drain.
  // (Closing the server first and awaiting it hangs after real traffic,
  // because a proxied socket lingers — and a live ably-js client, which
  // auto-reconnects, would just re-open one.)
  await supervisor.stop();
  // Now stop accepting new connections and force-drop any lingering sockets
  // so close() completes promptly.
  const closed = new Promise((r) => server.close(r));
  server.closeAllConnections?.();
  await closed;
  console.log('[express-app] clean shutdown complete');
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
