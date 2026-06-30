using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Yarp.ReverseProxy.Configuration;

namespace Ably.Embedded;

/// <summary>
/// One-call wiring for a host app: start the embedded <c>ably-server</c> and
/// reverse-proxy all traffic (REST + WebSocket) to it via YARP on the host's
/// public port. This is the entirety of the integration glue a developer
/// writes — see the example app's Program.cs.
/// </summary>
public static class AblyEmbeddedExtensions
{
    /// <summary>
    /// Registers the supervisor (as a singleton + hosted service so it starts
    /// with the app and stops with it) and a YARP reverse proxy whose single
    /// catch-all route forwards every path — including the WebSocket upgrade
    /// at <c>/</c> — to the embedded server's chosen loopback port.
    ///
    /// The destination address is resolved lazily from the running supervisor
    /// via an <see cref="InMemoryConfigProvider"/>, so the OS-assigned child
    /// port is injected without the developer hard-coding anything.
    /// </summary>
    public static IServiceCollection AddAblyEmbedded(
        this IServiceCollection services,
        Action<AblyServerOptions>? configure = null)
    {
        var options = new AblyServerOptions();
        configure?.Invoke(options);
        services.AddSingleton(options);
        services.AddSingleton<AblyServerSupervisor>();

        // Hosted service: StartAsync spins up the child + readiness gate before
        // the app reports started; StopAsync disposes it (SIGTERM, no orphan).
        services.AddSingleton<AblyEmbeddedHostedService>();
        services.AddHostedService(sp => sp.GetRequiredService<AblyEmbeddedHostedService>());

        // YARP, configured from an in-memory provider we update once the child
        // port is known. Until then the destination is a placeholder; the
        // hosted service overwrites it with the real loopback address before
        // marking the app ready.
        var provider = new InMemoryConfigProvider(BuildRoutes(), BuildClusters(destinationAddress: "http://127.0.0.1:1"));
        services.AddSingleton(provider);
        services.AddSingleton<IProxyConfigProvider>(sp => sp.GetRequiredService<InMemoryConfigProvider>());
        services.AddReverseProxy();

        return services;
    }

    /// <summary>
    /// Maps the YARP reverse proxy. WebSocket proxying is enabled by YARP out
    /// of the box (it forwards the upgrade, query string, headers and body
    /// unchanged), so a bare <c>MapReverseProxy()</c> covers both the realtime
    /// WS endpoint at <c>/</c> and every REST path.
    /// </summary>
    public static IEndpointRouteBuilder MapAblyEmbedded(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapReverseProxy();
        return endpoints;
    }

    private static IReadOnlyList<RouteConfig> BuildRoutes() =>
    [
        new RouteConfig
        {
            RouteId = "ably-embedded",
            ClusterId = "ably-embedded",
            // Catch-all: dedicated-port model (EMBEDDING-POC.md §6) — every
            // path (WS upgrade at '/', REST under /channels, /keys, /time,
            // /healthz, /readyz) routes to the embedded server.
            Match = new RouteMatch { Path = "/{**catch-all}" },
        },
    ];

    private static IReadOnlyList<ClusterConfig> BuildClusters(string destinationAddress) =>
    [
        new ClusterConfig
        {
            ClusterId = "ably-embedded",
            Destinations = new Dictionary<string, DestinationConfig>
            {
                ["embedded"] = new DestinationConfig { Address = destinationAddress },
            },
        },
    ];

    /// <summary>
    /// Rewrites the YARP cluster destination to the supervisor's actual
    /// loopback address. Called once the child port is known.
    /// </summary>
    internal static void PointAt(this InMemoryConfigProvider provider, Uri baseAddress) =>
        provider.Update(BuildRoutes(), BuildClusters(baseAddress.ToString()));

    /// <summary>
    /// Bridges the supervisor lifecycle into the host's hosted-service
    /// lifecycle and injects the resolved port into YARP before ready.
    /// </summary>
    private sealed class AblyEmbeddedHostedService(
        AblyServerSupervisor supervisor,
        InMemoryConfigProvider proxyConfig,
        ILogger<AblyEmbeddedHostedService> logger) : IHostedService
    {
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            await supervisor.StartAsync(cancellationToken).ConfigureAwait(false);
            proxyConfig.PointAt(supervisor.BaseAddress);
            logger.LogInformation("YARP now proxying to embedded ably-server at {Addr}", supervisor.BaseAddress);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            await supervisor.DisposeAsync().ConfigureAwait(false);
        }
    }
}
