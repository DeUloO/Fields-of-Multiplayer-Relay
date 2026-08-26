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

    public StatusReporter(string mpDir) => _path = Path.Combine(mpDir, "mp_status.json");

    public void Set(string state, string role, int peers, string? detail = null)
    {
        _state = state; _role = role; _peers = peers; _detail = detail;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await WriteOnceAsync(ct);
            try { await Task.Delay(500, ct); } catch { break; }
        }
    }

    public async Task WriteOnceAsync(CancellationToken ct)
    {
        var obj = new JsonObject
        {
            ["hb"] = Interlocked.Increment(ref _hb),
            ["state"] = _state,
            ["role"] = _role,
            ["peers"] = _peers,
            ["detail"] = _detail,
        };
        try
        {
            var tmp = _path + ".tmp";
            await File.WriteAllTextAsync(tmp, obj.ToJsonString(), ct);
            File.Move(tmp, _path, overwrite: true);
        }
        catch { }
    }

    public void TryDelete() { try { File.Delete(_path); } catch { } }
}
