// Express adapter: reverse-proxy ALL requests (HTTP + WS upgrade) to the
// embedded ably-server on a dedicated port.
//
// Because the PoC uses a DEDICATED PORT (EMBEDDING-POC.md §6), the proxy is a
// catch-all: the SDK speaks to the host on its own host+port and every path
// (`/` WS upgrade, `/channels/*`, `/keys/*`, `/time`, `/healthz`, `/readyz`)
// is forwarded verbatim to the child. Query string, headers and body are
// preserved by http-proxy-middleware.

import { createProxyMiddleware } from 'http-proxy-middleware';

/**
 * Create an Express middleware + an upgrade handler that proxy to the embedded
 * server. WebSocket upgrades are not seen by Express's normal middleware
 * stack, so the returned `upgrade` handler MUST be wired to the HTTP server:
 *
 *   const proxy = mountAblyProxy(app, { supervisor });
 *   const server = app.listen(PORT);
 *   server.on('upgrade', proxy.upgrade);
 *
 * @param {import('express').Express} app
 * @param {object} opts
 * @param {{ port: number }} opts.supervisor  the started supervisor (reads .port live)
 * @param {string} [opts.mountPath]  subpath to expose Ably under (default '/').
 *   When set (e.g. '/ably'), only that prefix is proxied and it is stripped
 *   before reaching the child, so the host app keeps the rest of its routes.
 * @param {(msg:string)=>void} [opts.log]
 * @returns {{ middleware: import('express').RequestHandler, upgrade: Function }}
 */
export function mountAblyProxy(app, opts) {
  const { supervisor } = opts;
  if (!supervisor) throw new Error('mountAblyProxy: opts.supervisor is required');

  const mountPath = (opts.mountPath || '/').replace(/\/$/, '') || '/';
  const subpath = mountPath !== '/';

  // router() is re-evaluated per request, so if the supervisor restarts the
  // child on a *new* port we still proxy to the live one.
  const proxy = createProxyMiddleware({
    router: () => `http://127.0.0.1:${supervisor.port}`,
    changeOrigin: true,
    ws: true,
    // Faithfully forward; do not buffer the (streamed) WS frames.
    xfwd: false,
    // Subpath mode: only proxy <mountPath>/* (HTTP and WS), and strip the
    // prefix so the child — which serves /, /channels, … at its root — sees
    // root-rooted paths. http-proxy-middleware applies both to WS upgrades.
    pathFilter: subpath ? (path) => path === mountPath || path.startsWith(mountPath + '/') : undefined,
    pathRewrite: subpath ? { ['^' + mountPath]: '' } : undefined,
    logger: opts.log ? { info: opts.log, warn: opts.log, error: opts.log } : undefined,
    on: {
      error(err, _req, res) {
        if (res && typeof res.writeHead === 'function' && !res.headersSent) {
          res.writeHead(502, { 'content-type': 'text/plain' });
          res.end(`embedded ably-server proxy error: ${err.message}`);
        }
      },
    },
  });

  app.use(proxy);

  return {
    middleware: proxy,
    // The 'upgrade' event handler; createProxyMiddleware exposes .upgrade.
    upgrade: proxy.upgrade,
  };
}
