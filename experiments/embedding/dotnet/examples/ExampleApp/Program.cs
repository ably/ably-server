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

// '/' (dedicated port) or e.g. '/ably' to mount under a subpath.
var mount = Environment.GetEnvironmentVariable("ABLY_MOUNT") ?? "/";

// One line of wiring: supervise the binary + configure YARP to proxy to it.
// The binary path defaults to <track>/bin/ably-server; override via
// ABLY_SERVER_BINARY for the PoC scripts.
builder.Services.AddAblyEmbedded(opts =>
{
    opts.BinaryPath = Environment.GetEnvironmentVariable("ABLY_SERVER_BINARY");
    opts.MountPath = mount;
    // ApiKey falls back to $ABLY_SERVER_API_KEY then app.key:secret.
});

var app = builder.Build();

// The host app's own routes sit alongside Ably. Serve the browser demo via
// terminal middleware (short-circuits BEFORE endpoint routing) so it never
// ties with YARP's catch-all route — registering it as an endpoint would
// throw AmbiguousMatchException against /{**catch-all}.
var demoHtml = ResolveDemoHtml();
app.Use(async (ctx, next) =>
{
    if (ctx.Request.Path.StartsWithSegments("/demo"))
    {
        ctx.Response.ContentType = "text/html";
        await ctx.Response.WriteAsync(demoHtml);
        return;
    }
    await next();
});
// A home page when Ably is under a subpath (root is free in that mode).
if (mount != "/")
{
    app.MapGet("/", () => Results.Text($"host app home — Ably is embedded under {mount}/"));
}

// Forward Ably traffic (REST + the WebSocket upgrade) to the embedded server.
// YARP handles the WS upgrade, query string, headers and body; under a
// subpath it strips the prefix first.
app.MapAblyEmbedded();

app.Run();

static string ResolveDemoHtml()
{
    var candidates = new[]
    {
        Environment.GetEnvironmentVariable("ABLY_DEMO_DIR"),
        Path.Combine(Directory.GetCurrentDirectory(), "experiments", "embedding", "demo"),
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "..", "demo")),
    };
    foreach (var dir in candidates)
    {
        if (string.IsNullOrEmpty(dir)) continue;
        var file = Path.Combine(dir, "index.html");
        if (File.Exists(file)) return File.ReadAllText(file);
    }
    return "<!doctype html><p>demo/index.html not found — set ABLY_DEMO_DIR.</p>";
}

static int GetPort(string[] args, int fallback)
{
    for (var i = 0; i < args.Length - 1; i++)
        if (args[i] is "--port" && int.TryParse(args[i + 1], out var p))
            return p;
    var env = Environment.GetEnvironmentVariable("ABLY_PUBLIC_PORT");
    return int.TryParse(env, out var ep) ? ep : fallback;
}
