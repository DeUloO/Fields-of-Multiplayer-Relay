using MomiMpRelay.Models;
using MomiMpRelay.Status;

namespace MomiMpRelay.Modes;

public sealed class AutoRelay
{
    readonly int _defaultPort;
    readonly string _mpDir;
    readonly string _remotePath;
    readonly StatusReporter _reporter;
    readonly Func<string, CancellationToken, Task<RelayControl?>> _readControl;
    readonly Func<int, CancellationToken, Task<int>> _runHost;
    readonly Func<string, int, CancellationToken, Task<int>> _runClient;

    public AutoRelay(int defaultPort, string mpDir, string remotePath, StatusReporter reporter,
        Func<string, CancellationToken, Task<RelayControl?>> readControl,
        Func<int, CancellationToken, Task<int>> runHost,
        Func<string, int, CancellationToken, Task<int>> runClient)
    {
        _defaultPort = defaultPort;
        _mpDir = mpDir;
        _remotePath = remotePath;
        _reporter = reporter;
        _readControl = readControl;
        _runHost = runHost;
        _runClient = runClient;
    }

    public async Task<int> RunAsync(CancellationToken ct)
    {
        var path = Path.Combine(_mpDir, "mp_control.json");
        long lastSeq = -1;
        CancellationTokenSource? sessionCts = null;
        Task? sessionTask = null;

        async Task TearDownAsync()
        {
            if (sessionCts is null) return;
            await sessionCts.CancelAsync();
            if (sessionTask is not null) try { await sessionTask; } catch { }
            sessionCts.Dispose();
            sessionCts = null;
            sessionTask = null;
            try { File.Delete(_remotePath); } catch { }
        }

        _reporter.Set("idle", "off", 0);
        while (!ct.IsCancellationRequested)
        {
            var control = await _readControl(path, ct);
            if (control is { } value && value.Seq != lastSeq)
            {
                lastSeq = value.Seq;
                await TearDownAsync();
                var port = value.Port > 0 ? value.Port : _defaultPort;
                if (value.Mode == "host")
                {
                    _reporter.Set("listening", "host", 0);
                    sessionCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    sessionTask = _runHost(port, sessionCts.Token);
                }
                else if (value.Mode == "join")
                {
                    _reporter.Set("connecting", "join", 0);
                    sessionCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    sessionTask = _runClient(value.Ip, port, sessionCts.Token);
                }
                else _reporter.Set("idle", "off", 0);
            }
            try { await Task.Delay(50, ct); } catch (OperationCanceledException) { break; }
        }
        await TearDownAsync();
        return 0;
    }
}
