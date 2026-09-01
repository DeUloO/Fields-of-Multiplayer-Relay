using System.Text.Json.Nodes;

namespace MomiMpRelay.Status;

public sealed class StatusReporter
{
    readonly string _path;
    long _hb;
    string _state = "idle";
    string _role = "off";
    int _peers;
    string? _detail;
    long _ledgerHeadRelaySeq;
    IReadOnlyDictionary<string, long> _clientLag = new Dictionary<string, long>();
    int _outboxPending;
    int _inboxPending;

    public StatusReporter(string mpDir) => _path = Path.Combine(mpDir, "mp_status.json");

    public void Set(string state, string role, int peers, string? detail = null)
    {
        _state = state;
        _role = role;
        _peers = peers;
        _detail = detail;
    }

    /// <summary>Surfaces mutation-pipeline health so problems are visible without reading the SQLite ledger directly.</summary>
    public void SetMutationDiagnostics(long ledgerHeadRelaySeq, IReadOnlyDictionary<string, long> clientLag, int outboxPending, int inboxPending)
    {
        _ledgerHeadRelaySeq = ledgerHeadRelaySeq;
        _clientLag = clientLag;
        _outboxPending = outboxPending;
        _inboxPending = inboxPending;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await WriteOnceAsync(ct);
            try
            {
                await Task.Delay(500, ct);
            }
            catch { break; }
        }
    }

    public async Task WriteOnceAsync(CancellationToken ct)
    {
        var clientLag = new JsonObject();
        foreach (var (playerId, lag) in _clientLag)
            clientLag[playerId] = lag;

        var obj = new JsonObject
        {
            ["hb"] = Interlocked.Increment(ref _hb),
            ["state"] = _state,
            ["role"] = _role,
            ["peers"] = _peers,
            ["detail"] = _detail,
            ["ledgerHeadRelaySeq"] = _ledgerHeadRelaySeq,
            ["clientLag"] = clientLag,
            ["outboxPending"] = _outboxPending,
            ["inboxPending"] = _inboxPending,
        };
        try
        {
            var tmp = _path + ".tmp";
            await File.WriteAllTextAsync(tmp, obj.ToJsonString(), ct);
            File.Move(tmp, _path, overwrite: true);
        }
        catch { }
    }

    public void TryDelete()
    {
        try
        {
            File.Delete(_path);
        }
        catch { }
    }
}
