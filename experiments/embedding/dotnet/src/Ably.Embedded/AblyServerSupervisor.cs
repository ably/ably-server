using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ably.Embedded;

/// <summary>
/// Supervises the prebuilt <c>ably-server</c> binary as a child process:
/// resolves the binary, picks a free loopback TCP port, starts the server in
/// <c>memory</c> mode bound to that port, polls <c>/readyz</c> until ready,
/// restarts the child if it exits unexpectedly, and stops it (SIGTERM then
/// Kill) on <see cref="DisposeAsync"/> — leaving no orphan process.
///
/// This is the integration glue a .NET host-app developer consumes; the
/// supervised port is then injected into a YARP cluster destination (see
/// <see cref="AblyEmbeddedExtensions"/>).
/// </summary>
public sealed class AblyServerSupervisor : IAsyncDisposable
{
    private readonly AblyServerOptions _options;
    private readonly ILogger _logger;
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(2) };
    private readonly object _gate = new();

    private Process? _process;
    private CancellationTokenSource? _superviseCts;
    private Task? _superviseTask;
    private volatile bool _stopping;
    private int _disposed; // 0 = live, 1 = disposed (idempotent guard)

    /// <summary>The loopback port the child server is bound to.</summary>
    public int Port { get; private set; }

    /// <summary>The base URI of the embedded server, e.g. <c>http://127.0.0.1:54321</c>.</summary>
    public Uri BaseAddress => new($"http://127.0.0.1:{Port}");

    /// <summary>The resolved API key actually passed to the child.</summary>
    public string ApiKey { get; }

    /// <summary>The resolved absolute path to the binary actually launched.</summary>
    public string ResolvedBinaryPath { get; }

    public AblyServerSupervisor(AblyServerOptions options, ILogger<AblyServerSupervisor>? logger = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = (ILogger?)logger ?? NullLogger.Instance;
        ApiKey = ResolveApiKey(options);
        ResolvedBinaryPath = ResolveBinaryPath(options);
    }

    /// <summary>
    /// Picks a free port, starts the child, waits for <c>/readyz</c>, and
    /// arms the auto-restart watchdog. Throws if the server never becomes
    /// ready within <see cref="AblyServerOptions.ReadyTimeout"/>.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        Port = PickFreePort();
        _superviseCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        await SpawnAsync(cancellationToken).ConfigureAwait(false);
        await WaitForReadyAsync(_options.ReadyTimeout, cancellationToken).ConfigureAwait(false);

        // Watchdog: respawn the child if it dies unexpectedly.
        _superviseTask = SuperviseLoopAsync(_superviseCts.Token);

        _logger.LogInformation(
            "ably-server ready on {BaseAddress} (pid {Pid}, binary {Binary})",
            BaseAddress, _process?.Id, ResolvedBinaryPath);
    }

    private async Task SpawnAsync(CancellationToken cancellationToken)
    {
        var psi = new ProcessStartInfo
        {
            FileName = ResolvedBinaryPath,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        psi.ArgumentList.Add("--mode");
        psi.ArgumentList.Add(_options.Mode);
        psi.ArgumentList.Add("--listen");
        psi.ArgumentList.Add($"127.0.0.1:{Port}");
        psi.ArgumentList.Add("--log-level");
        psi.ArgumentList.Add(_options.LogLevel);
        psi.ArgumentList.Add("--shutdown-grace");
        psi.ArgumentList.Add($"{(int)_options.ShutdownGrace.TotalSeconds}s");
        if (!string.IsNullOrEmpty(_options.DataDir))
        {
            psi.ArgumentList.Add("--data-dir");
            psi.ArgumentList.Add(_options.DataDir);
        }
        if (!string.IsNullOrEmpty(_options.DbDsn))
        {
            psi.ArgumentList.Add("--db-dsn");
            psi.ArgumentList.Add(_options.DbDsn);
        }
        psi.Environment["ABLY_SERVER_API_KEY"] = ApiKey;

        var process = new Process { StartInfo = psi, EnableRaisingEvents = true };
        // Forward the child's logs at a visible level so the embedded server
        // is not silent under the host's default (Information/Warning) filter.
        process.OutputDataReceived += (_, e) => { if (e.Data is not null) _logger.LogInformation("[ably-server] {Line}", e.Data); };
        process.ErrorDataReceived += (_, e) => { if (e.Data is not null) _logger.LogWarning("[ably-server] {Line}", e.Data); };

        if (!process.Start())
            throw new InvalidOperationException($"failed to start ably-server at {ResolvedBinaryPath}");

        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        Process? previous;
        lock (_gate) { previous = _process; _process = process; }
        // Release the OS handle of the prior (already-exited) child on respawn.
        previous?.Dispose();
        _logger.LogInformation("spawned ably-server pid {Pid} on 127.0.0.1:{Port}", process.Id, Port);
        await Task.CompletedTask;
    }

    /// <summary>
    /// Watches the current child; on an unexpected exit (we are not stopping
    /// and restart is enabled) it respawns and re-waits for readiness. The
    /// child keeps the same <see cref="Port"/>, so the YARP destination stays
    /// valid across a restart.
    /// </summary>
    private async Task SuperviseLoopAsync(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            Process? current;
            lock (_gate) current = _process;
            if (current is null) return;

            try
            {
                await current.WaitForExitAsync(token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return; // we're shutting down
            }

            if (_stopping || token.IsCancellationRequested) return;
            if (!_options.RestartOnExit)
            {
                _logger.LogWarning("ably-server pid {Pid} exited (code {Code}); restart disabled", current.Id, SafeExitCode(current));
                return;
            }

            _logger.LogWarning(
                "ably-server pid {Pid} exited unexpectedly (code {Code}); respawning on port {Port}",
                current.Id, SafeExitCode(current), Port);

            try
            {
                await Task.Delay(_options.RestartDelay, token).ConfigureAwait(false);
                await SpawnAsync(token).ConfigureAwait(false);
                await WaitForReadyAsync(_options.ReadyTimeout, token).ConfigureAwait(false);
                _logger.LogInformation("ably-server respawned and ready on {BaseAddress} (pid {Pid})", BaseAddress, _process?.Id);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "failed to respawn ably-server; retrying");
            }
        }
    }

    private async Task WaitForReadyAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        var readyUri = new Uri(BaseAddress, "/readyz");
        var deadline = DateTime.UtcNow + timeout;
        Exception? last = null;
        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // If the child died during startup, fail fast with its exit code.
            Process? p;
            lock (_gate) p = _process;
            if (p is not null && p.HasExited)
                throw new InvalidOperationException($"ably-server exited during startup (code {SafeExitCode(p)})");

            try
            {
                using var resp = await _http.GetAsync(readyUri, cancellationToken).ConfigureAwait(false);
                if (resp.IsSuccessStatusCode) return;
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
            {
                last = ex;
            }
            await Task.Delay(100, cancellationToken).ConfigureAwait(false);
        }
        throw new TimeoutException($"ably-server did not become ready at {readyUri} within {timeout.TotalSeconds:0.#}s", last);
    }

    /// <summary>Current child PID, or null if not running. Used by tests/fault-injection.</summary>
    public int? CurrentPid
    {
        get { lock (_gate) return _process is { HasExited: false } p ? p.Id : null; }
    }

    public async ValueTask DisposeAsync()
    {
        // Idempotent: the supervisor is both a hosted service (StopAsync ->
        // DisposeAsync) and a DI-owned singleton (the container also disposes
        // IAsyncDisposable singletons on shutdown). Guard so the second call
        // is a no-op instead of touching the already-disposed CTS.
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;

        _stopping = true;
        _superviseCts?.Cancel();

        if (_superviseTask is not null)
        {
            try { await _superviseTask.ConfigureAwait(false); } catch { /* best effort */ }
        }

        Process? process;
        lock (_gate) { process = _process; _process = null; }

        if (process is not null)
            await StopProcessAsync(process).ConfigureAwait(false);

        _superviseCts?.Dispose();
        _http.Dispose();
    }

    /// <summary>
    /// Stops a child gracefully: SIGTERM (so the server drains and exits 0
    /// within its grace window), waiting up to the grace period, then a hard
    /// Kill as a last resort. Guarantees no orphaned process survives dispose.
    /// </summary>
    private async Task StopProcessAsync(Process process)
    {
        if (process.HasExited) return;
        var pid = process.Id;
        try
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                // No SIGTERM on Windows; Kill (entire tree) is the clean stop.
                process.Kill(entireProcessTree: true);
            }
            else
            {
                // POSIX: send SIGTERM and let the server's graceful shutdown run.
                _ = kill(pid, SIGTERM);
            }

            using var cts = new CancellationTokenSource(_options.ShutdownGrace);
            try
            {
                await process.WaitForExitAsync(cts.Token).ConfigureAwait(false);
                _logger.LogInformation("ably-server pid {Pid} stopped cleanly (code {Code})", pid, SafeExitCode(process));
                return;
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("ably-server pid {Pid} did not exit within grace; killing", pid);
            }

            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "error stopping ably-server pid {Pid}", pid);
            try { if (!process.HasExited) process.Kill(entireProcessTree: true); } catch { /* ignore */ }
        }
        finally
        {
            process.Dispose();
        }
    }

    private static int SafeExitCode(Process p)
    {
        try { return p.HasExited ? p.ExitCode : -1; } catch { return -1; }
    }

    private static string ResolveApiKey(AblyServerOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.ApiKey)) return options.ApiKey!;
        var env = Environment.GetEnvironmentVariable("ABLY_SERVER_API_KEY");
        return string.IsNullOrWhiteSpace(env) ? "app.key:secret" : env!;
    }

    /// <summary>
    /// Resolves the binary path. Explicit <see cref="AblyServerOptions.BinaryPath"/>
    /// wins; otherwise we probe a few conventional locations relative to the
    /// running app (this is the "extract per-RID native" precedent NuGet uses
    /// via <c>runtimes/&lt;rid&gt;/native/</c> — not implemented here, see NOTES.md).
    /// </summary>
    private static string ResolveBinaryPath(AblyServerOptions options)
    {
        var exe = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "ably-server.exe" : "ably-server";

        if (!string.IsNullOrWhiteSpace(options.BinaryPath))
        {
            var explicitPath = Path.GetFullPath(options.BinaryPath!);
            if (!File.Exists(explicitPath))
                throw new FileNotFoundException($"ably-server binary not found at configured BinaryPath: {explicitPath}");
            return explicitPath;
        }

        var baseDir = AppContext.BaseDirectory;
        var candidates = new[]
        {
            Path.Combine(baseDir, exe),
            Path.Combine(baseDir, "bin", exe),
            // dotnet/bin/ably-server relative to the track root (dev layout):
            Path.Combine(baseDir, "..", "..", "..", "..", "..", "bin", exe),
            Path.Combine(Directory.GetCurrentDirectory(), "bin", exe),
        };
        foreach (var c in candidates)
        {
            var full = Path.GetFullPath(c);
            if (File.Exists(full)) return full;
        }

        throw new FileNotFoundException(
            $"could not resolve the ably-server binary ('{exe}'). Set AblyServerOptions.BinaryPath. " +
            $"Probed: {string.Join(", ", candidates.Select(Path.GetFullPath))}");
    }

    /// <summary>
    /// Binds an OS-assigned ephemeral loopback port, then releases it so the
    /// child can claim it. There is a tiny TOCTOU window, acceptable for a PoC
    /// (the alternative — a stdout ready-line carrying the bound port — is the
    /// deferred, more robust option noted in EMBEDDING-POC.md §8).
    /// </summary>
    private static int PickFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private const int SIGTERM = 15;

    [DllImport("libc", SetLastError = true)]
    private static extern int kill(int pid, int sig);
}
