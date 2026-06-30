// Native ably-js smoke test: prove the UNMODIFIED ably npm SDK works through
// the host-app proxy. Point the SDK at the public port with tls:false and
// assert a publish->subscribe round-trip on a realtime channel.
//
// Usage: ABLY_PUBLIC_PORT=8541 node examples/ably-js-smoke.js

import * as Ably from 'ably';

const PORT = Number(process.env.ABLY_PUBLIC_PORT ?? 8541);
const KEY = process.env.ABLY_SERVER_API_KEY ?? 'app.key:secret';

const client = new Ably.Realtime({
  key: KEY,
  restHost: '127.0.0.1',
  realtimeHost: '127.0.0.1',
  port: PORT,
  tls: false,
  // keep logs quiet; bump to 4 for debugging
  logLevel: 0,
});

const TIMEOUT_MS = 15_000;
const fail = (msg) => {
  console.error(`SMOKE FAIL: ${msg}`);
  client.close();
  process.exit(1);
};
const timer = setTimeout(() => fail(`timed out after ${TIMEOUT_MS}ms`), TIMEOUT_MS);

try {
  await client.connection.once('connected');
  console.log('[smoke] connection state: connected');

  const channel = client.channels.get('embed-poc-smoke');
  const payload = { hello: 'ably-js', ts: Date.now(), nonce: Math.random().toString(36).slice(2) };

  const received = new Promise((resolve, reject) => {
    channel.subscribe('greeting', (msg) => {
      try {
        if (msg.data && msg.data.nonce === payload.nonce) resolve(msg.data);
        else reject(new Error(`payload mismatch: ${JSON.stringify(msg.data)}`));
      } catch (e) {
        reject(e);
      }
    });
  });

  await channel.attach();
  console.log('[smoke] channel attached, publishing...');
  await channel.publish('greeting', payload);

  const got = await received;
  if (got.nonce !== payload.nonce) fail('round-trip payload mismatch');
  console.log(`[smoke] round-trip OK: ${JSON.stringify(got)}`);

  clearTimeout(timer);
  client.close();
  console.log('SMOKE PASS: unmodified ably-js published and subscribed through the proxy');
  process.exit(0);
} catch (err) {
  clearTimeout(timer);
  fail(err && err.message ? err.message : String(err));
}
