// Public entrypoint for @ably/embedded-server (PoC).
//
//   import { startEmbeddedServer } from '@ably/embedded-server';
//   import { mountAblyProxy } from '@ably/embedded-server/express';
//   import { registerAblyProxy } from '@ably/embedded-server/fastify';

export {
  AblyServerSupervisor,
  startEmbeddedServer,
  resolveBinaryPath,
  pickFreePort,
  waitForReady,
} from './supervise.js';
