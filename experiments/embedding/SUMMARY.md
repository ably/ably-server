# Embedding ably-server: what it proves, and why it matters

**Bottom line:** we can ship a real, protocol-compatible Ably endpoint *inside*
someone else's web app, as a package they include rather than a server they
run. I had it working through Go, Node, .NET and Python. An unmodified Ably
SDK points at the host app and gets pub/sub, history and resume-after-drop. No
separate service to deploy, no extra infrastructure, no client changes. You
include a package, and you're done.

This isn't a hack, and it isn't novel. Plenty of well-known projects do exactly
this (esbuild, Prisma, Playwright, mongodb-memory-server, dapr, Testcontainers).
What's new is that *Ably* can do it. The measured proof, per language and per
dimension, is in [RESULTS.md](RESULTS.md); how to run it is in
[USING.md](USING.md); and there's a live browser demo at
[demo/index.html](demo/index.html) that you can open in two tabs and watch
realtime flow through an endpoint your own web server is hosting.

## What it looks like

Two ends to it: the host app embeds Ably on its own port, and an unmodified
Ably SDK points at that port. Node and Python below; the others (Go in-process,
.NET via YARP, Fastify) are in [USING.md](USING.md).

**Node (Express).** Embed it:

```js
import express from 'express';
import { startEmbeddedServer } from '@ably/embedded-server';
import { mountAblyProxy } from '@ably/embedded-server/express';

const supervisor = await startEmbeddedServer({ apiKey: 'app.key:secret' });
const app = express();
const proxy = mountAblyProxy(app, { supervisor });   // Ably on this app's port
const server = app.listen(8541);
server.on('upgrade', proxy.upgrade);                  // forward WebSocket upgrades
```

Use it, with an unmodified ably-js (browser or Node):

```js
import * as Ably from 'ably';
const ably = new Ably.Realtime({
  key: 'app.key:secret',
  restHost: '127.0.0.1', realtimeHost: '127.0.0.1', port: 8541, tls: false,
});
const channel = ably.channels.get('demo');
channel.subscribe('msg', (m) => console.log(m.data));
await channel.publish('msg', 'hello from an embedded Ably');
```

**Python (FastAPI).** Embed it:

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from ably_embedded import AblyServer, make_proxy_app

server = AblyServer(api_key="app.key:secret")   # mode="disk", data_dir=... for durability

@asynccontextmanager
async def lifespan(app):
    await server.start()            # spawn the child, wait for /readyz
    yield
    await server.stop()             # stop it with the app, no orphan

app = FastAPI(lifespan=lifespan)
proxy = make_proxy_app(server)      # catch-all ASGI reverse-proxy (WS + REST)
# your own routes live on `app`; Ably traffic falls through to `proxy`
# (see examples/fastapi_app.py for the ASGI fall-through wiring)
```

Use it, with an unmodified ably-python:

```python
from ably import AblyRealtime
# ably-python folds host:port into realtime_host (it ignores the `port` option)
ably = AblyRealtime(key="app.key:secret", realtime_host="127.0.0.1:8581", tls=False)
channel = ably.channels.get("demo")
await channel.subscribe("greeting", lambda m: print(m.data))
await channel.publish("greeting", "hello from an embedded Ably")
```

## Why I tried this

Paddy's [PDR-090](https://ably.atlassian.net/wiki/spaces/product/pages/5171281935/PDR-090+Source-available+Ably+implementation)
makes the case for a source-available Ably server, and I agree with where it
lands: Option C, a freely usable local server as standard Ably tooling, as the
on-ramp into the cloud. This experiment is the existence proof for one framing
in that doc that I think is underweighted: **embeddability**. Not "a server you
can self-host", but "a thing you drop into the app you're already building".

## The bit missing from the PDR

PDR-090 frames the competition as Centrifugo and Sockudo: servers you stand up
and run. Those are real and the doc is right about them. But the thing
developers reach for *first*, and increasingly the coding agent reaching for it
on their behalf, is Socket.IO. And Socket.IO's appeal was never that it's a
server you run. It's that you `npm install socket.io` and it's *part of your
app*. Nothing else to deploy. That's the bar, and it's a different axis from
"self-hosted server".

This PoC shows we can meet that bar. Not identically, and I want to be precise
about that. With Socket.IO, or SignalR on .NET, you write code against a
connection API: you own the socket, you handle the events. With embedded
ably-server you write no connection code at all. Your unmodified Ably SDK talks
to a real Ably endpoint that happens to be running in your process. So it isn't
the same thing. But it's the same *spirit*, include a package and you're done,
and for what we care about it's the better deal: you get the whole Ably
protocol and every Ably SDK, not a bespoke socket library you'll outgrow.

## Where it gets interesting: AI

This is the part I care about most. If you're building an AI application, an
embedded ably-server gives you resumable streams and the AI Transport
semantics running locally inside your app. And it can be durable: in disk mode
the state is a local file, so sessions and history survive a restart. We
proved that end to end (publish, `kill -9`, restart on the same data dir, all
messages still there, see `results/disk-durability.log`), and the Go embed API
takes `Mode: "disk"` for it. Socket.IO has nothing like this. It doesn't scale
the way the cloud does, and shared state across instances still needs cluster
mode (Postgres), but as a starting point it lets people build realtime AI apps
in a better way than they do today, without installing any extra services.
Open source, protocol-compatible, with a clean upgrade path to Ably cloud when
they outgrow it. That's a strong on-ramp.

## What it is, and what it isn't

It's an on-ramp. It is explicitly *not* meant to be a robust, at-scale
production service. That distinction matters, because it answers the obvious
objections:

- **Serverless and edge are out as an embed target, and this one's worth being
  precise about because it's changed.** Vercel Functions now support
  WebSockets and Socket.IO (on Fluid compute), and could even bundle and spawn
  the binary; Netlify Functions still don't hold persistent WebSockets. But
  WebSockets were never the real blocker. A function instance is ephemeral,
  duration-capped (connections drop at the 800s/1800s limit) and autoscaled,
  so an embedded server would reset and fragment into isolated nodes across
  instances, which is why Vercel itself tells you to keep shared state in an
  external store. That a WebSocket-capable serverless platform still doesn't
  fit the embed is the clearest proof of where this belongs: the embed's home
  is a long-lived host (a container, a VM, a dev machine, a desktop app), and
  scale belongs on the cloud. On serverless you point the SDK at a long-lived
  ably-server or Ably cloud, which isn't embedding. Detail and sources are in
  [USING.md](USING.md) and [RESULTS.md](RESULTS.md).
- **Scale is the upgrade, not the job.** A single embedded node in memory mode
  is one isolated Ably. ably-server *does* scale, that's the design: stateless
  nodes in front of a shared Postgres (`cluster` mode), and it works. But the
  moment you're running Postgres and orchestrating replicas, you've left the
  "just a package" sweet spot, and that's exactly the point where the honest
  question is "why not just use the cloud service?" So the embed shines for
  local dev, CI, demos, and getting a simple app reasonably far. Past that, you
  graduate to Ably.
- **"People moved in-process for a reason."** True, Prisma and others moved
  off a supervised child binary to reduce cold-start and serverless friction.
  That's a fair concern when the thing is your critical production data path.
  It mostly doesn't apply here, because this isn't trying to be that. It's a
  dev and on-ramp tool. And where in-process genuinely matters, Go already gives
  it: import the package, mount the handler, no child process at all.

## It doesn't cannibalise anything

If anything it protects the funnel. The only customers who could self-host
instead of paying are low-volume self-service ones, and they're already at
risk: today they pick Socket.IO or Centrifugo and never enter the Ably funnel
at all. Giving them a protocol-compatible Ably they can start with, for free,
inside their own app, turns a lost prospect into someone already building on
our SDKs and protocol, with the managed service as the natural next step. That
maps to PDR-090's Option C, an on-ramp, not Option D, competing with Centrifugo
on the features of a self-hosted server. The embeddable ergonomic makes that
on-ramp stronger than a "run our dev server" story, because there's nothing to
run.

## What I'd take back to the PDR

One line: add the embeddable axis to the competitive analysis. We're not only
answering Centrifugo and Sockudo (servers you run); we're answering Socket.IO
and SignalR (libraries you include), and that's the comparison that decides
what a developer, or their coding agent, picks on day one.
