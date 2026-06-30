# Embedding ably-server in a .NET app (M3 — .NET + YARP)

This track proves an **unmodified Ably .NET SDK** can reach an **embedded**
`ably-server` through a .NET host app. The host supervises the prebuilt
server binary as a child process and reverse-proxies realtime (WebSocket) +
REST traffic to it on a **dedicated port** using
[YARP](https://github.com/dotnet/yarp). See
[EMBEDDING-POC.md](../../../EMBEDDING-POC.md) §5–§8 for the model and
[NOTES.md](./NOTES.md) for the measured trade-offs.

## What's here

```
dotnet/
  AblyEmbedded.slnx
  src/Ably.Embedded/        # the reusable glue (NuGet-shaped library)
    AblyServerSupervisor.cs   - resolve binary, free port, spawn, /readyz gate,
                                restart-on-crash, SIGTERM shutdown (no orphan)
    AblyEmbeddedExtensions.cs - AddAblyEmbedded() + MapAblyEmbedded() (YARP wiring)
    AblyServerOptions.cs      - config surface
  examples/
    ExampleApp/             # minimal ASP.NET host (THE integration glue a dev writes)
    SdkSmoke/               # console app using the unmodified IO.Ably SDK
  scripts/                  # the verification scripts (conformance, smoke, fault)
  bin/ably-server           # the prebuilt server binary (gitignored)
```

## Prerequisites

- **.NET SDK 8+** (built and run here on **.NET SDK 10.0.301**; only the
  net10.0 runtime is installed on this machine — see [NOTES.md](./NOTES.md)).
- The prebuilt `ably-server` binary at `dotnet/bin/ably-server`. Build it
  from the repo root:

  ```sh
  go build -o experiments/embedding/dotnet/bin/ably-server ./cmd/ably-server
  ```

- The conformance harness (for the verification step):

  ```sh
  go build -o experiments/embedding/harness/harness ./experiments/embedding/harness
  ```

## The integration glue (what a host-app developer writes)

This is the **entire** wiring — `examples/ExampleApp/Program.cs`. Everything
else (supervision, free port, readiness gate, restart-on-crash, graceful
shutdown, WebSocket proxying) is in the `Ably.Embedded` package:

```csharp
using Ably.Embedded;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls("http://127.0.0.1:8561"); // the dedicated public port

// One call: supervise the binary + configure YARP to proxy to it.
builder.Services.AddAblyEmbedded(opts =>
{
    opts.ApiKey = "app.key:secret";   // or env ABLY_SERVER_API_KEY
    // opts.BinaryPath = ...;          // defaults to <app>/bin/ably-server
});

var app = builder.Build();
app.MapAblyEmbedded();                  // catch-all route: REST + WS upgrade
app.Run();
```

That's **2 calls** (`AddAblyEmbedded` + `MapAblyEmbedded`) on top of the
stock ASP.NET minimal-host template — ~23 non-comment lines total including
the port plumbing the PoC scripts need.

Point any Ably SDK at the host's public port with TLS off:

```csharp
// IO.Ably (the official Ably .NET SDK, NuGet package id "ably.io")
var realtime = new AblyRealtime(new ClientOptions
{
    Key = "app.key:secret",
    RealtimeHost = "127.0.0.1", RestHost = "127.0.0.1",
    Port = 8561, TlsPort = 8561, Tls = false,
});
```

> **REST caveat (real finding):** IO.Ably 1.2.18 refuses **basic-auth (API
> key) REST over plain HTTP** and exposes no opt-out. Realtime/WebSocket is
> unaffected (it auth's via the WS query string). Use **token auth**
> (`UseTokenAuth = true`) for REST over the non-TLS port. Full detail in
> [NOTES.md](./NOTES.md#dx).

## Build

```sh
cd experiments/embedding/dotnet
dotnet build -c Release
```

## Run the example app

```sh
ABLY_SERVER_API_KEY=app.key:secret \
ABLY_SERVER_BINARY="$PWD/bin/ably-server" \
dotnet examples/ExampleApp/bin/Release/net10.0/ExampleApp.dll --port 8561
```

The host listens on `127.0.0.1:8561`; the embedded server gets its own
OS-assigned free loopback port. `Ctrl+C` / `SIGTERM` shuts the host down and
stops the child (no orphan).

## Verify (the scripts capture real output)

All three scripts live in `scripts/` and are self-contained:

```sh
# A. Conformance: run the Go harness THROUGH the .NET+YARP host.
#    Writes experiments/embedding/results/dotnet-yarp.json (exit 0 = all six pass).
./scripts/run-conformance.sh 8561

# B. Native Ably .NET SDK smoke through YARP (realtime round-trip + REST finding).
./scripts/run-sdk-smoke.sh 8562

# C. Fault injection:
#    C1 kill -9 the child  -> supervisor respawns it, fresh harness passes.
#    C2 SIGTERM the app    -> clean exit (0), no orphan child.
./scripts/run-fault-injection.sh 8563
```

Use ports in the **8560–8579** range (a sibling Node track runs on 8540+).

## Configuration surface (`AblyServerOptions`)

| Option | Default | Meaning |
|---|---|---|
| `ApiKey` | `$ABLY_SERVER_API_KEY` → `app.key:secret` | `appId.keyId:keySecret` |
| `BinaryPath` | `<app>/bin/ably-server` (+ fallbacks) | absolute path to the binary |
| `Mode` | `memory` | server `--mode` |
| `LogLevel` | `error` | server `--log-level` |
| `ShutdownGrace` | `10s` | `--shutdown-grace` + stop wait before Kill |
| `ReadyTimeout` | `15s` | how long to poll `/readyz` on startup |
| `RestartOnExit` | `true` | respawn the child if it dies unexpectedly |
| `RestartDelay` | `200ms` | delay before respawn |
