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
    /// Registers the embedded server (as a singleton + hosted service so it
    /// starts with the app and stops with it) and a YARP reverse proxy whose
    /// single catch-all route forwards every path — including the WebSocket
    /// upgrade at <c>/</c> — to the embedded server's chosen loopback port.
    ///
    /// The destination address is resolved lazily from the running server
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
        services.AddSingleton<AblyServer>();

        // Hosted service: StartAsync spins up the child + readiness gate before
        // the app reports started; StopAsync disposes it (SIGTERM, no orphan).
        services.AddSingleton<AblyEmbeddedHostedService>();
        services.AddHostedService(sp => sp.GetRequiredService<AblyEmbeddedHostedService>());

        // YARP, configured from an in-memory provider we update once the child
        // port is known. Until then the destination is a placeholder; the
        // hosted service overwrites it with the real loopback address before
        // marking the app ready.
        var provider = new InMemoryConfigProvider(BuildRoutes(options.MountPath), BuildClusters(destinationAddress: "http://127.0.0.1:1"));
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

    private static IReadOnlyList<RouteConfig> BuildRoutes(string mountPath)
    {
        var mount = mountPath.TrimEnd('/');
        if (mount.Length == 0)
        {
            // Dedicated-port model (EMBEDDING-POC.md §6): catch-all — every
            // path (WS upgrade at '/', REST under /channels, /keys, /time,
            // /healthz, /readyz) routes to the embedded server.
            return
            [
                new RouteConfig
                {
                    RouteId = "ably-embedded",
                    ClusterId = "ably-embedded",
                    Match = new RouteMatch { Path = "/{**catch-all}" },
                },
            ];
        }
        // Subpath model: match only <mount>/** and strip the prefix so the
        // child sees root-rooted paths. PathRemovePrefix applies to the
        // WebSocket upgrade too.
        return
        [
            new RouteConfig
            {
                RouteId = "ably-embedded",
                ClusterId = "ably-embedded",
                Match = new RouteMatch { Path = mount + "/{**catch-all}" },
                Transforms = new[]
                {
                    new Dictionary<string, string> { ["PathRemovePrefix"] = mount },
                },
            },
        ];
    }

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
    /// Rewrites the YARP cluster destination to the embedded server's actual
    /// loopback address. Called once the child port is known.
    /// </summary>
    internal static void PointAt(this InMemoryConfigProvider provider, Uri baseAddress, string mountPath) =>
        provider.Update(BuildRoutes(mountPath), BuildClusters(baseAddress.ToString()));

    /// <summary>
    /// Bridges the embedded server's lifecycle into the host's hosted-service
    /// lifecycle and injects the resolved port into YARP before ready.
    /// </summary>
    private sealed class AblyEmbeddedHostedService(
        AblyServer server,
        InMemoryConfigProvider proxyConfig,
        AblyServerOptions options,
        ILogger<AblyEmbeddedHostedService> logger) : IHostedService
    {
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            await server.StartAsync(cancellationToken).ConfigureAwait(false);
            proxyConfig.PointAt(server.BaseAddress, options.MountPath);
            logger.LogInformation("YARP now proxying to embedded ably-server at {Addr} (mount {Mount})", server.BaseAddress, options.MountPath);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            await server.DisposeAsync().ConfigureAwait(false);
        }
    }
}
