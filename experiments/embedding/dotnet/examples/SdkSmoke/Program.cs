using System.Diagnostics;
using IO.Ably;
using IO.Ably.Realtime;

// Native Ably .NET SDK smoke test.
//
// Uses the UNMODIFIED official Ably .NET SDK (NuGet 'ably.io', namespace
// IO.Ably) pointed at the host app's PUBLIC port over plain HTTP/WS. A pass
// proves a stock SDK reaches the embedded ably-server THROUGH YARP and can
// attach + publish + subscribe with a round-trip echo.
//
// Usage: SdkSmoke <publicPort> [apiKey]

var port = args.Length > 0 && int.TryParse(args[0], out var p) ? p : 8561;
var key = args.Length > 1 ? args[1]
    : Environment.GetEnvironmentVariable("ABLY_SERVER_API_KEY") ?? "app.key:secret";
var host = "127.0.0.1";

Console.Error.WriteLine($"[sdk-smoke] connecting IO.Ably v{typeof(AblyRealtime).Assembly.GetName().Version} to {host}:{port} (tls=off)");

// Point the stock SDK at host+port with TLS off — exactly the wiring a host
// app would use for the dedicated-port embedding model (EMBEDDING-POC.md §6).
var options = new ClientOptions
{
    Key = key,
    RealtimeHost = host,
    RestHost = host,
    Port = port,
    TlsPort = port,
    Tls = false,
    UseBinaryProtocol = false, // JSON, matches the harness
    UseTokenAuth = false,      // basic auth with the key over the local socket
    AutoConnect = false,
    EchoMessages = true,       // we publish and await our own echo
    LogLevel = LogLevel.Error,
};

var realtime = new AblyRealtime(options);

var connected = new TaskCompletionSource();
var failed = new TaskCompletionSource<string>();
realtime.Connection.On(ConnectionEvent.Connected, _ => connected.TrySetResult());
realtime.Connection.On(ConnectionEvent.Failed, change =>
    failed.TrySetResult(change.Reason?.ToString() ?? "connection failed"));

var sw = Stopwatch.StartNew();
realtime.Connect();

var connectResult = await Task.WhenAny(connected.Task, failed.Task, Task.Delay(TimeSpan.FromSeconds(15)));
if (connectResult == failed.Task)
    return Fail($"connection FAILED: {await failed.Task}");
if (!connected.Task.IsCompleted)
    return Fail($"timeout waiting for CONNECTED (state={realtime.Connection.State})");
Console.Error.WriteLine($"[sdk-smoke] CONNECTED in {sw.ElapsedMilliseconds}ms (state={realtime.Connection.State})");

var channelName = $"dotnet-sdk-smoke-{Guid.NewGuid():N}";
var channel = realtime.Channels.Get(channelName);

var roundTrip = new TaskCompletionSource<string>();
channel.Subscribe("greeting", msg =>
{
    if (msg.Data is string s) roundTrip.TrySetResult(s);
});

await channel.AttachAsync();
Console.Error.WriteLine($"[sdk-smoke] ATTACHED to {channelName} (state={channel.State})");

const string payload = "hello-from-unmodified-dotnet-sdk";
var t0 = Stopwatch.StartNew();
var pubResult = await channel.PublishAsync("greeting", payload);
if (pubResult.IsFailure)
    return Fail($"publish failed: {pubResult.Error}");

var rtResult = await Task.WhenAny(roundTrip.Task, Task.Delay(TimeSpan.FromSeconds(10)));
if (!roundTrip.Task.IsCompleted)
    return Fail("timeout waiting for the published message to echo back over WS");

var received = await roundTrip.Task;
if (received != payload)
    return Fail($"round-trip data mismatch: got '{received}', want '{payload}'");

Console.Error.WriteLine($"[sdk-smoke] round-trip OK in {t0.ElapsedMilliseconds}ms: published and received '{received}'");

// --- The realtime round-trip above is the brief's required proof, and it
// passes through YARP with the unmodified SDK. The REST side is a SEPARATE,
// documented finding. ---
//
// IO.Ably 1.2.18's REST client hard-refuses BASIC (API-key) auth over plain
// HTTP via AblyAuth.EnsureSecureConnection(), and there is NO ClientOptions
// flag to allow it (unlike ably-go's WithInsecureAllowBasicAuthWithoutTLS()).
// The realtime/WS transport is exempt because it authenticates with the key
// in the WS query string, not the HTTP auth header — which is why connect /
// attach / pubsub all worked above. So a key-auth REST history call over the
// non-TLS YARP port throws. We demonstrate the finding here (without failing
// the smoke), then prove REST *is* reachable through YARP using TOKEN auth,
// which bypasses the basic-auth-over-HTTP guard.
var restFinding = "n/a";
var histCount = -1;
try
{
    var restBasic = new AblyRest(options); // key + Tls=false => basic auth over http
    var h = await restBasic.Channels.Get(channelName).HistoryAsync();
    histCount = h.Items.Count;
    Console.Error.WriteLine($"[sdk-smoke] REST(basic) history through YARP returned {histCount} message(s)");
    restFinding = "basic-auth REST over http unexpectedly allowed";
}
catch (AblyInsecureRequestException ex)
{
    restFinding = "basic-auth REST over http blocked by SDK (EnsureSecureConnection); no ClientOptions opt-out";
    Console.Error.WriteLine($"[sdk-smoke] FINDING: {restFinding} -> {ex.ErrorInfo}");
}

// Workaround: token auth lets the same SDK do REST over plain HTTP through YARP.
try
{
    var tokenOptions = new ClientOptions
    {
        Key = key,
        RestHost = host,
        Port = port,
        TlsPort = port,
        Tls = false,
        UseBinaryProtocol = false,
        UseTokenAuth = true, // request a token from /keys/.../requestToken, then bearer-auth
        LogLevel = LogLevel.Error,
    };
    var restToken = new AblyRest(tokenOptions);
    var ht = await restToken.Channels.Get(channelName).HistoryAsync();
    histCount = ht.Items.Count;
    Console.Error.WriteLine($"[sdk-smoke] REST(token) history through YARP returned {histCount} message(s) — workaround OK");
    restFinding += "; token-auth REST over http works through YARP";
}
catch (Exception ex)
{
    Console.Error.WriteLine($"[sdk-smoke] token-auth REST workaround failed: {ex.Message}");
    restFinding += $"; token-auth workaround failed: {ex.Message.Replace("\"", "'")}";
}

realtime.Close();
Console.Error.WriteLine("[sdk-smoke] PASS (realtime pub/sub round-trip through YARP)");
Console.WriteLine($"{{\"pass\":true,\"connectMs\":{sw.ElapsedMilliseconds},\"roundTripMs\":{t0.ElapsedMilliseconds},\"historyCount\":{histCount},\"channel\":\"{channelName}\",\"restFinding\":\"{restFinding}\"}}");
return 0;

static int Fail(string message)
{
    Console.Error.WriteLine($"[sdk-smoke] FAIL: {message}");
    Console.WriteLine($"{{\"pass\":false,\"error\":\"{message.Replace("\"", "'")}\"}}");
    return 1;
}
