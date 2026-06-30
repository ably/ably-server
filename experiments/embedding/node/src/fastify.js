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
 *   await registerAblyProxy(fastify, { supervisor });
 *   await fastify.listen({ port: PORT, host: '0.0.0.0' });
 *
 * @param {import('fastify').FastifyInstance} fastify
 * @param {object} opts
 * @param {{ port: number }} opts.supervisor the started supervisor
 * @returns {Promise<void>}
 */
export async function registerAblyProxy(fastify, opts) {
  const { supervisor } = opts;
  if (!supervisor) throw new Error('registerAblyProxy: opts.supervisor is required');

  await fastify.register(fastifyHttpProxy, {
    // The child's port is fixed for the life of a given child. @fastify/http-proxy
    // resolves `upstream` once at registration; the supervisor keeps the SAME
    // internal port across crash-restarts (it reuses supervisor.port), so this
    // stays valid through a respawn.
    upstream: `http://127.0.0.1:${supervisor.port}`,
    prefix: '/',
    rewritePrefix: '/',
    websocket: true,
    // Preserve the request as-is; do not strip or rewrite the query string.
    replyOptions: {
      // keep original host header semantics simple for a dedicated port
    },
  });
}
