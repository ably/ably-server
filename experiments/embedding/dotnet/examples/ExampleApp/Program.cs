using Ably.Embedded;
using Microsoft.Extensions.Logging;

// Example host app: embed ably-server behind YARP on a dedicated public port.
//
// This file IS the integration glue a host-app developer writes. Everything
// else (supervision, free-port pick, readiness gate, restart-on-crash,
// graceful shutdown, WebSocket proxying) lives in the Ably.Embedded library.

var builder = WebApplication.CreateBuilder(args);

// Public port the host listens on; the embedded server gets its own free
// loopback port. --urls / ASPNETCORE_URLS also work; we honour an explicit
// --port for the PoC scripts.
var publicPort = GetPort(args, fallback: 8561);
builder.WebHost.UseUrls($"http://127.0.0.1:{publicPort}");

// Keep the host quiet but surface the supervisor's lifecycle + lifetime
// banner (so "Application started/stopping" and the embedded-server pid show).
builder.Logging.SetMinimumLevel(LogLevel.Warning);
builder.Logging.AddFilter("Ably.Embedded", LogLevel.Information);
builder.Logging.AddFilter("Microsoft.Hosting.Lifetime", LogLevel.Information);

// One line of wiring: supervise the binary + configure YARP to proxy to it.
// The binary path defaults to <track>/bin/ably-server; override via
// ABLY_SERVER_BINARY for the PoC scripts.
builder.Services.AddAblyEmbedded(opts =>
{
    opts.BinaryPath = Environment.GetEnvironmentVariable("ABLY_SERVER_BINARY");
    // ApiKey falls back to $ABLY_SERVER_API_KEY then app.key:secret.
});

var app = builder.Build();

// Forward everything (REST + the WebSocket upgrade at '/') to the embedded
// server. YARP handles the WS upgrade, query string, headers and body.
app.MapAblyEmbedded();

app.Run();

static int GetPort(string[] args, int fallback)
{
    for (var i = 0; i < args.Length - 1; i++)
        if (args[i] is "--port" && int.TryParse(args[i + 1], out var p))
            return p;
    var env = Environment.GetEnvironmentVariable("ABLY_PUBLIC_PORT");
    return int.TryParse(env, out var ep) ? ep : fallback;
}
