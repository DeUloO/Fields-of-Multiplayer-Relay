namespace MomiMpRelay.Snapshots;

using MomiMpRelay.Models;

public sealed class SnapshotReceiver : IDisposable
{
    readonly string _mpDir;
    readonly Dictionary<string, FileStream> _open = new();
    readonly Dictionary<string, int> _nextChunk = new();
    readonly Dictionary<string, int> _expectedChunks = new();
    readonly Dictionary<string, int> _expectedBytes = new();
    readonly Dictionary<string, int> _receivedBytes = new();
    readonly HashSet<string> _activeFiles = new();
    readonly HashSet<string> _completedFiles = new();
    bool _snapshotInProgress;

    public SnapshotReceiver(string mpDir) => _mpDir = mpDir;

    public async Task HandleAsync(IMpControlMessage message, CancellationToken ct)
    {
        switch (message)
        {
            case SnapshotBegin begin:
            {
                var name = begin.Name;
                if (!TryGetFileId(name, out _)
                    || begin.Chunks < 0 || begin.Bytes < 0
                    || (begin.Bytes == 0 && begin.Chunks != 0)
                    || (begin.Bytes > 0 && begin.Chunks == 0))
                {
                    Console.WriteLine($"[CLIENT] Invalid snapshot metadata for {name}; ignoring.");
                    break;
                }
                CloseOne(name);
                if (!_snapshotInProgress)
                {
                    _completedFiles.Clear();
                    _snapshotInProgress = true;
                }
                var part = Path.Combine(_mpDir, name + ".part");
                _open[name] = new FileStream(part, FileMode.Create, FileAccess.Write,
                    FileShare.None, 1 << 16, useAsync: true);
                _nextChunk[name] = 0;
                _expectedChunks[name] = begin.Chunks;
                _expectedBytes[name] = begin.Bytes;
                _receivedBytes[name] = 0;
                _activeFiles.Add(name);
                Console.WriteLine($"[CLIENT] Receiving {name} ({begin.Bytes} bytes)…");
                break;
            }
            case SnapshotEnd end:
            {
                var name = end.Name;
                if (!_open.ContainsKey(name) ||
                    !_nextChunk.TryGetValue(name, out var received) ||
                    !_expectedChunks.TryGetValue(name, out var expected) ||
                    received != expected ||
                    !_expectedBytes.TryGetValue(name, out var expectedBytes) ||
                    !_receivedBytes.TryGetValue(name, out var receivedBytes) ||
                    receivedBytes != expectedBytes)
                {
                    Console.WriteLine($"[CLIENT] Incomplete snapshot {name}; ignoring.");
                    CloseOne(name, deletePart: true);
                    break;
                }
                CloseOne(name);
                _activeFiles.Remove(name);
                _completedFiles.Add(name);
                var part = Path.Combine(_mpDir, name + ".part");
                var final = Path.Combine(_mpDir, name);
                try { if (File.Exists(part)) File.Move(part, final, overwrite: true); }
                catch (Exception ex) { Console.WriteLine($"[CLIENT] snap_end {name}: {ex.Message}"); }
                break;
            }
            case SnapshotDone:
                if (!_snapshotInProgress || _activeFiles.Count != 0 ||
                    !_completedFiles.Contains("world_snapshot.json"))
                {
                    Console.WriteLine("[CLIENT] Snapshot completion rejected; files are incomplete.");
                    CleanupSnapshot(deleteParts: true);
                    break;
                }
                try
                {
                    await File.WriteAllTextAsync(Path.Combine(_mpDir, "mp_apply_world"), "", ct);
                    Console.WriteLine("[CLIENT] World snapshot complete → apply requested.");
                    _snapshotInProgress = false;
                    _completedFiles.Clear();
                }
                catch (Exception ex) { Console.WriteLine($"[CLIENT] snap_done: {ex.Message}"); }
                break;
        }
    }

    public async Task HandleChunkAsync(byte fileId, int sequence, byte[] bytes,
        CancellationToken ct)
    {
        var name = fileId switch
        {
            1 => "world_snapshot.json",
            2 => "world_farm_terrain.bin",
            _ => null,
        };
        if (name is null || !_open.TryGetValue(name, out var fs) ||
            !_nextChunk.TryGetValue(name, out var expected) || sequence != expected ||
            !_expectedBytes.TryGetValue(name, out var expectedBytes) ||
            !_receivedBytes.TryGetValue(name, out var receivedBytes) ||
            bytes.Length > expectedBytes - receivedBytes)
        {
            if (name is not null && _open.ContainsKey(name)) CloseOne(name, deletePart: true);
            return;
        }

        await fs.WriteAsync(bytes, ct);
        _nextChunk[name] = expected + 1;
        _receivedBytes[name] = receivedBytes + bytes.Length;
    }

    static bool TryGetFileId(string name, out byte fileId)
    {
        fileId = name switch
        {
            "world_snapshot.json" => (byte)1,
            "world_farm_terrain.bin" => (byte)2,
            _ => (byte)0,
        };
        return fileId != 0;
    }

    void CloseOne(string name, bool deletePart = false)
    {
        if (_open.TryGetValue(name, out var fs))
        {
            try { fs.Dispose(); } catch { }
            _open.Remove(name);
            _nextChunk.Remove(name);
            _expectedChunks.Remove(name);
            _expectedBytes.Remove(name);
            _receivedBytes.Remove(name);
            _activeFiles.Remove(name);
            if (deletePart)
            {
                try { File.Delete(Path.Combine(_mpDir, name + ".part")); } catch { }
            }
        }
    }

    void CleanupSnapshot(bool deleteParts)
    {
        foreach (var name in _open.Keys.ToArray()) CloseOne(name, deleteParts);
        _activeFiles.Clear();
        _completedFiles.Clear();
        _snapshotInProgress = false;
    }

    public void Dispose() => CleanupSnapshot(deleteParts: true);
}
