// Fastify adapter: reverse-proxy ALL requests (HTTP + WS upgrade) to the
// embedded ably-server on a dedicated port, via @fastify/http-proxy.
//
// Dedicated-port catch-all (EMBEDDING-POC.md §6): upstream is the child, prefix
// is '/', websocket proxying is on. @fastify/http-proxy forwards the upgrade,
// query string, headers and body unchanged.

import fastifyHttpProxy from '@fastify/http-proxy';

/**
 * Register the Ably reverse proxy on a Fastify instance.
 *
 *   const fastify = Fastify();
 *   await registerAblyProxy(fastify, { server });
 *   await fastify.listen({ port: PORT, host: '0.0.0.0' });
 *
 * @param {import('fastify').FastifyInstance} fastify
 * @param {object} opts
 * @param {{ port: number }} opts.server the started AblyServer
 * @param {{ port: number }} [opts.supervisor] deprecated alias for opts.server
 * @param {string} [opts.mountPath] subpath to expose Ably under (default '/').
 *   '/ably' proxies only that prefix and strips it (rewritePrefix '/'), so the
 *   host app keeps the rest of its routes.
 * @returns {Promise<void>}
 */
export async function registerAblyProxy(fastify, opts) {
  const server = opts.server ?? opts.supervisor;
  if (!server) throw new Error('registerAblyProxy: opts.server is required');

  const mountPath = (opts.mountPath || '/').replace(/\/$/, '') || '/';

  await fastify.register(fastifyHttpProxy, {
    // The child's port is fixed for the life of a given child. @fastify/http-proxy
    // resolves `upstream` once at registration; the server keeps the SAME
    // internal port across crash-restarts (it reuses server.port), so this
    // stays valid through a respawn.
    upstream: `http://127.0.0.1:${server.port}`,
    // prefix is the public mount point; rewritePrefix '/' strips it so the
    // child sees root-rooted paths. @fastify/http-proxy applies this to the
    // WebSocket upgrade too.
    prefix: mountPath,
    rewritePrefix: '/',
    websocket: true,
    replyOptions: {},
  });
}
