namespace Ably.Embedded;

/// <summary>
/// Configuration for the embedded <c>ably-server</c> child process. Every
/// field has a working default; a host app typically only sets
/// <see cref="ApiKey"/> (or relies on the <c>ABLY_SERVER_API_KEY</c> env var).
/// </summary>
public sealed class AblyServerOptions
{
    /// <summary>
    /// API key in <c>appId.keyId:keySecret</c> form. If null, the supervisor
    /// falls back to the <c>ABLY_SERVER_API_KEY</c> environment variable, then
    /// to the PoC default <c>app.key:secret</c>.
    /// </summary>
    public string? ApiKey { get; set; }

    /// <summary>
    /// Absolute path to the prebuilt <c>ably-server</c> binary. If null, the
    /// supervisor resolves it next to the host app's content root under
    /// <c>bin/ably-server</c> (plus a few common fallbacks). See
    /// <see cref="AblyServerSupervisor"/> for the resolution order.
    /// </summary>
    public string? BinaryPath { get; set; }

    /// <summary>Storage mode passed as <c>--mode</c>. Defaults to <c>memory</c>.</summary>
    public string Mode { get; set; } = "memory";

    /// <summary>Server log level passed as <c>--log-level</c>.</summary>
    public string LogLevel { get; set; } = "error";

    /// <summary>
    /// Graceful-shutdown window passed as <c>--shutdown-grace</c>. The
    /// supervisor also waits up to this long for the child to exit on its own
    /// SIGTERM before force-killing.
    /// </summary>
    public TimeSpan ShutdownGrace { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>How long to poll <c>/readyz</c> before giving up on startup.</summary>
    public TimeSpan ReadyTimeout { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// When true (default) the supervisor respawns the child if it exits
    /// without having been asked to stop. Set false to disable auto-restart.
    /// </summary>
    public bool RestartOnExit { get; set; } = true;

    /// <summary>Delay before respawning after an unexpected exit.</summary>
    public TimeSpan RestartDelay { get; set; } = TimeSpan.FromMilliseconds(200);
}
