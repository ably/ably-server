// Public entrypoint for @ably/embedded-server (PoC).
//
//   import { AblyServer, startEmbeddedServer } from '@ably/embedded-server';
//   import { mountAblyProxy } from '@ably/embedded-server/express';
//   import { registerAblyProxy } from '@ably/embedded-server/fastify';

// AblyServer is the server object a host holds; startEmbeddedServer is the
// one-call convenience. AblyServerSupervisor is a back-compat alias.
// (resolveBinaryPath / pickFreePort / waitForReady are internal helpers and
// are intentionally not part of the public surface.)
export {
  AblyServer,
  AblyServerSupervisor,
  startEmbeddedServer,
} from './supervise.js';
