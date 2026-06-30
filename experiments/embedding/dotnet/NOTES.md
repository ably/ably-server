# M3 .NET + YARP — measured trade-offs (deliverable for M4)

Measured on macOS (Darwin arm64, .NET SDK 10.0.301, Go 1.25.11). The host
app embeds the prebuilt `ably-server` binary as a child process and
reverse-proxies all realtime (WebSocket) + REST traffic to it via YARP on a
dedicated public port. Numbers below are **measured**, not estimated; raw
artifacts are in `experiments/embedding/results/`.

## Headline result

The **Go conformance harness passes through .NET + YARP** — all six
scenarios, exit 0:

```
== harness [dotnet-yarp] against 127.0.0.1:8561 (protocol=json) ==
  ok   connect          5ms  connectMs=4.98
  ok   pubsub           5ms  maxMs=0.27 meanMs=0.2 minMs=0.17 p50Ms=0.19 p99Ms=0.27 samples=20
  ok   restpubsub       6ms  maxMs=1.45 meanMs=0.5 minMs=0.34 p50Ms=0.43 p99Ms=1.45 samples=10
  ok   history          4ms  count=10 ordered=true
  ok   resume           8ms  cursor=...@e6b6387030 delivered=3 gapSize=3 lossless=true resumedFlag=true
  ok   soak           104ms  duplicates=0 lost=0 messages=200 throughputPerSec=1974.06
== PASS [dotnet-yarp] (132ms wall) ==
```

`resume` passing is the key proof: YARP forwards a fresh WS upgrade with the
query string intact and proxies the protocol frames unmangled, so the
server's resume-after-drop path (RESUMED flag + exact gap replay, zero loss)
works identically through the proxy. Raw report:
`results/dotnet-yarp.json`.

---

## §7 dimensions

### Developer efficiency (time-to-first-message)

Following the README path on this machine (NuGet packages warm in the global
cache, server binary prebuilt):

| Step | Command | Measured |
|---|---|---|
| Restore (warm cache) | `dotnet restore` | **0.47 s** |
| Build (3 projects, Release) | `dotnet build -c Release` | **0.72 s** |
| Start host + native SDK pub/sub round-trip | `run-sdk-smoke.sh` | **0.87 s** |
| **Total, clean build → working round-trip** | | **~1.6 s** |

Within the SDK round-trip: the unmodified IO.Ably client reached
**CONNECTED in ~58 ms** and completed a publish→subscribe **round-trip in
~6 ms** through YARP.

Step count from a prebuilt binary: **3** — (1) `dotnet build`, (2) run the
host, (3) point an SDK at the port. Honest caveat: a true *first-ever* run
also pays a one-time NuGet download (YARP 2.4 MB + IO.Ably 13 MB extracted;
see Portability) and the one-time `go build` of the server binary (~not
counted here, it is a build input). The 1.6 s figure is the warm-cache
inner loop a developer actually iterates on.

### DX

- **Integration glue the host-app dev writes:** `examples/ExampleApp/Program.cs`
  is **48 lines total, 23 non-comment/non-blank**, of which the
  Ably-specific surface is exactly **2 calls**: `AddAblyEmbedded(...)` and
  `MapAblyEmbedded()`. Everything else is the stock ASP.NET minimal-host
  template. The reusable package is 509 LOC the developer does **not** write.
- **New concepts:** essentially the idiomatic ASP.NET ones a .NET dev
  already knows — `IServiceCollection` extension + endpoint mapping +
  `IHostedService` lifecycle. YARP's in-memory config provider is the one
  mildly non-obvious piece, and it is hidden inside the package. Fits
  idiomatic YARP config cleanly (a catch-all `RouteConfig`/`ClusterConfig`).
- **WebSocket proxying is free.** A bare `MapReverseProxy()` forwards the WS
  upgrade, query string, headers and body unchanged — no custom middleware,
  no manual `Accept-WebSocket` handling. This is the dimension where .NET is
  strongest (EMBEDDING-POC.md §4).
- **Misconfiguration error quality (actual output):**
  - Bad binary path →
    `System.IO.FileNotFoundException: ably-server binary not found at configured BinaryPath: /nonexistent/ably-server`
    (the resolver also lists the probed candidate paths when none is set).
  - Invalid API key → the child exits during startup and the supervisor
    **fails fast** instead of hanging:
    `System.InvalidOperationException: ably-server exited during startup (code 1)`.
  Both are clear and actionable; neither hangs to the ready-timeout.
- **The one real SDK finding — REST over plain HTTP:** the official Ably
  .NET SDK **IO.Ably 1.2.18** refuses **basic-auth (API-key) REST over
  `http://`** via `AblyAuth.EnsureSecureConnection()`, and reflection over
  the assembly confirms **no `ClientOptions` opt-out exists** (no
  `Insecure*`/`AllowHttp*` member anywhere — unlike `ably-go`'s
  `WithInsecureAllowBasicAuthWithoutTLS()`). The exact thrown error:

  ```
  IO.Ably.AblyInsecureRequestException: [ErrorInfo Reason: Current action
  cannot be performed over http (See https://help.ably.io/error/500);
  Code: 500; Href: https://help.ably.io/error/500]
     at IO.Ably.AblyAuth.EnsureSecureConnection()
  ```

  **Scope of the limitation:** realtime/WebSocket is **unaffected** — it
  authenticates with the key in the WS query string, not the HTTP auth
  header — which is why connect/attach/**pub/sub all pass** through YARP with
  the unmodified SDK. Only the SDK's *REST* client over the non-TLS port is
  blocked. **Workaround (proven):** set `UseTokenAuth = true`; the SDK then
  fetches a token from `POST /keys/.../requestToken` and bearer-auths, which
  the guard permits. The smoke test runs this and REST history succeeds
  through YARP (returns the message we published over realtime — proving
  cross-protocol persistence through the proxy). This is a *host-side wiring*
  consideration for productisation: for a same-machine embedded server,
  either ship the host with TLS on the dedicated port, or default the SDK
  guidance to token auth. It is **not** a YARP/proxy limitation (the Go
  harness, using `ably-go` with the opt-out, does key-auth REST through the
  same YARP route fine).

### Portability

- **Server binary:** `bin/ably-server` = **16,288,242 bytes (~16 MB)**,
  Mach-O `darwin/arm64`. One per OS/arch (the Go cross-compile matrix);
  per-platform packages multiply registry footprint as flagged in §10.
- **Managed glue assembly:** `Ably.Embedded.dll` = **~30 KB** (tiny; the
  weight is the native binary, not the wrapper).
- **Toolchain to run:** a **.NET runtime ≥ 8** (the ASP.NET shared framework
  for YARP) — heavier than Node's "just the binary", lighter than needing a
  full SDK. The `Ably.Embedded` library targets `net8.0`; the example app
  and smoke test target `net10.0` **only because that is the single runtime
  installed on this PoC machine** (`dotnet --list-runtimes` shows
  `Microsoft.AspNetCore.App 10.0.9` / `Microsoft.NETCore.App 10.0.9` and no
  .NET 8 runtime). The code uses no API newer than net8.0; on a box with the
  .NET 8 runtime the example app's TFM can drop to `net8.0` unchanged.
- **NuGet packaging precedent (noted, not implemented):** NuGet ships native
  assets under `runtimes/<rid>/native/` and extracts the right RID at
  runtime — the same pattern esbuild uses on npm. A productised
  `Ably.Embedded` NuGet would bundle `runtimes/osx-arm64/native/ably-server`,
  `runtimes/linux-x64/native/ably-server`, `runtimes/win-x64/native/ably-server.exe`,
  and the supervisor's `ResolveBinaryPath` would probe `AppContext.BaseDirectory`
  (already a candidate it checks). Multi-RID packaging is out of PoC scope
  per the brief; the resolver is structured so adding it is a packaging
  change, not a code change.
- **Download footprint, first run:** YARP **2.4 MB**, IO.Ably **13 MB**
  (extracted in the NuGet cache).

### Ease of use

| Ergonomic | This track |
|---|---|
| One-liner to start? | Yes — `AddAblyEmbedded()` + `MapAblyEmbedded()` (2 calls); host runs with one `dotnet` command. |
| Auto free-port for the child? | Yes — supervisor binds an OS ephemeral loopback port and injects it into the YARP destination. The dev never picks the internal port. |
| Auto-shutdown with the app? | Yes — wired as an `IHostedService`; `StopAsync` SIGTERMs the child and waits out the grace window (no orphan). Verified (C2). |
| Restart on crash? | Yes — watchdog respawns on unexpected exit, same port, re-gated on `/readyz`. Verified (C1). |
| Config surface | A single `AblyServerOptions` (flags-style), with env fallbacks (`ABLY_SERVER_API_KEY`, and `ABLY_SERVER_BINARY` honoured by the example app). |

### Reliability

**Conformance (through YARP):** PASS, all six scenarios incl. **resume**
(RESUMED flag, lossless 3-message gap replay) and **soak** (200 messages,
**0 lost, 0 duplicates**, ~1,974 msg/s through the proxy). See the headline
block above; raw: `results/dotnet-yarp.json`.

**Fault injection (run, not described — `results/dotnet-fault-injection.log`):**

- **C1 — `kill -9` the embedded child:** the supervisor detected the exit
  (logged `exited unexpectedly (code 137); respawning`), respawned a **new
  PID on the same port**, re-gated on `/readyz`, and a fresh
  `harness --scenarios connect,pubsub` **PASSED** against the respawned
  child. Observed: `old pid 29739 dead, new pid 29753 alive, /readyz green`.
- **C2 — SIGTERM the host app:** host exited **cleanly (code 0)**, logged
  `ably-server pid ... stopped cleanly (code 0)`, and the embedded child was
  **gone — no orphan** (verified by PID liveness check). The graceful path
  sends SIGTERM to the child and waits the grace window before any Kill.

One correctness bug found + fixed during this work: the supervisor is both a
hosted service *and* a DI-owned `IAsyncDisposable` singleton, so the
container disposed it twice on shutdown → `ObjectDisposedException` on the
cancellation-token source (non-zero process exit). Fixed by making
`DisposeAsync` idempotent (`Interlocked` guard). After the fix, C2 exits 0.

### Complexity / operational cost

- **Moving parts added to a deployment:** one extra child process per host
  instance (the embedded server) + YARP in-process. Failure blast-radius is
  **isolated** — a server crash kills the child, not the host, and the
  watchdog restarts it (vs an in-process Go mount where a panic could take
  the whole host down).
- **Supervision burden:** owned by the package (spawn, ready-gate, restart,
  shutdown). The host dev writes none of it. The one watch-out is the
  **free-port TOCTOU**: we bind-then-release an ephemeral port and hand it to
  the child; a tiny race exists. The brief's deferred "stdout ready-line
  carrying the bound port" would remove it; not needed for the PoC.
- **Packaging complexity:** moderate — a productised NuGet must bundle
  per-RID native binaries (`runtimes/<rid>/native/`) and carries macOS
  notarisation / Windows code-signing costs (flagged in §10, out of PoC
  scope). The managed wrapper itself is trivial (~30 KB, 509 LOC).
- **Single embedded node = no shared state** (§10): two host instances each
  embed their own `memory`-mode server and won't see each other's channels.
  Scale path is `cluster` mode (shared Postgres); not tested here by design.

---

## How to reproduce

```sh
# from repo root: build inputs
go build -o experiments/embedding/dotnet/bin/ably-server ./cmd/ably-server
go build -o experiments/embedding/harness/harness ./experiments/embedding/harness

cd experiments/embedding/dotnet
dotnet build -c Release
./scripts/run-conformance.sh 8561        # -> results/dotnet-yarp.json (exit 0)
./scripts/run-sdk-smoke.sh   8562        # native IO.Ably through YARP (exit 0)
./scripts/run-fault-injection.sh 8563    # C1 + C2 (exit 0)
```
