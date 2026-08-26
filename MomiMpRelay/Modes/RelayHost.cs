using System.Collections.Concurrent;
using System.Text;
using System.Text.Json.Nodes;
using LiteNetLib;
using MomiMpRelay.FileSystem;
using MomiMpRelay.Logging;
using MomiMpRelay.Models;
using MomiMpRelay.Networking;
using MomiMpRelay.Status;

namespace MomiMpRelay.Modes;

public sealed class RelayHost
{
    const int PollMs = 50, ChunkBytes = 900, SnapshotTimeoutMs = 20_000;
    readonly int _port; readonly string _mpDir, _outPath, _remotePath; readonly StatusReporter _reporter;

    public RelayHost(int port, string mpDir, string outPath, string remotePath, StatusReporter reporter)
    { _port = port; _mpDir = mpDir; _outPath = outPath; _remotePath = remotePath; _reporter = reporter; }

    public async Task<int> RunAsync(CancellationToken ct)
    {
        var states = new ConcurrentDictionary<string, JsonObject>();
        var sessions = new ConcurrentDictionary<ClientSession, byte>();
        using var writeLock = new SemaphoreSlim(1, 1);
        using var snapshotLock = new SemaphoreSlim(1, 1);
        string? hostPid = null;
        void PushToAll()
        {
            foreach (var (session, _) in sessions)
                if (session.PlayerId is { } pid)
                    if (!session.Push(new RelayStateUpdate(JsonNode.Parse(BuildRemoteJson(states, pid))!.AsObject())))
                        RelayLogger.Error($"[HOST] Outbox full for {session.Peer.Address}; state update dropped.");
        }
        void Refresh() => _reporter.Set("listening", "host", sessions.Count);
        var net = new NetManager(new RelayListener(
            (peer, address) =>
            {
                var session = new ClientSession(peer); sessions.TryAdd(session, 0); Refresh();
                RelayLogger.Info($"[HOST] + {address}");
                _ = Task.Run(() => WriteLoop(session, ct), ct);
                _ = Task.Run(() => ReadLoop(session, ct), ct);
            },
            (peer, packet) =>
            {
                var session = sessions.Keys.FirstOrDefault(s => s.Peer == peer);
                if (session is not null && !session.Inbox.Writer.TryWrite(packet))
                {
                    RelayLogger.Error($"[HOST] Inbox full for {peer.Address}; disconnecting slow peer.");
                    peer.Disconnect();
                }
            },
            (peer, disconnect) =>
            {
                var session = sessions.Keys.FirstOrDefault(s => s.Peer == peer); if (session is null) return;
                session.Inbox.Writer.TryComplete();
                if (session.PlayerId is { } pid) states.TryRemove(pid, out _);
                sessions.TryRemove(session, out _); session.Dispose(); Refresh();
                RelayLogger.Info($"[HOST] Disconnected {peer.Address}: {disconnect.Reason}");
            }));
        net.Start(_port);
        using var pollCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var pollTask = Task.Run(async () => { while (!pollCts.IsCancellationRequested) { net.PollEvents(); try { await Task.Delay(10, pollCts.Token); } catch (OperationCanceledException) { } } }, pollCts.Token);
        _reporter.Set("listening", "host", 0); RelayLogger.Info($"[HOST] Listening on :{_port}");
        try
        {
            string? last = null;
            while (!ct.IsCancellationRequested)
            {
                var raw = await RelayFileStore.ReadTextSharedAsync(_outPath, ct);
                if (raw is not null && raw != last)
                {
                    last = raw; var state = PlayerState.Parse(raw);
                    if (state is not null) { hostPid = state.PlayerId; states[state.PlayerId] = state.Payload; await RelayFileStore.WriteRemoteAsync(_remotePath, BuildRemoteJson(states, hostPid), writeLock, ct); PushToAll(); }
                }
                await Task.Delay(PollMs, ct);
            }
        }
        catch (OperationCanceledException) { }
        finally { await pollCts.CancelAsync(); try { await pollTask; } catch (OperationCanceledException) { } net.Stop(); foreach (var (s, _) in sessions) s.Dispose(); }
        return 0;

        async Task ReadLoop(ClientSession session, CancellationToken token)
        {
            try
            {
                await foreach (var packet in session.Inbox.Reader.ReadAllAsync(token))
                {
                    if (packet.Kind != RelayPacketKind.Json) continue;
                    var message = RelayMessageParser.Parse(Encoding.UTF8.GetString(packet.Data));
                    if (message is SnapshotRequest) { await SendSnapshot(session, token); continue; }
                    if (message is not PlayerState state) continue;
                    session.PlayerId = state.PlayerId; states[state.PlayerId] = state.Payload;
                    if (hostPid is not null) await RelayFileStore.WriteRemoteAsync(_remotePath, BuildRemoteJson(states, hostPid), writeLock, token);
                    PushToAll();
                }
            }
            catch (OperationCanceledException) { }
        }
        async Task SendSnapshot(ClientSession session, CancellationToken token)
        {
            byte[]? world; byte[]? terrain;
            await snapshotLock.WaitAsync(token);
            try
            {
                var request = Path.Combine(_mpDir, "mp_snap_request"); var ready = Path.Combine(_mpDir, "mp_snap_ready");
                try { File.Delete(ready); } catch { }
                await File.WriteAllTextAsync(request, "{}", token); var start = DateTime.UtcNow;
                while (!File.Exists(ready)) { if ((DateTime.UtcNow - start).TotalMilliseconds > SnapshotTimeoutMs) return; await Task.Delay(100, token); }
                try { File.Delete(ready); } catch { }
                world = await RelayFileStore.ReadBytesSharedAsync(Path.Combine(_mpDir, "world_snapshot.json"), token);
                terrain = await RelayFileStore.ReadBytesSharedAsync(Path.Combine(_mpDir, "world_farm_terrain.bin"), token);
            }
            finally { snapshotLock.Release(); }
            if (world is null) return; await SendFile(session, "world_snapshot.json", world, token); if (terrain is not null) await SendFile(session, "world_farm_terrain.bin", terrain, token); await RelayTransport.SendLockedAsync(session, new SnapshotDone(), token);
        }
        async Task SendFile(ClientSession session, string name, byte[] bytes, CancellationToken token)
        {
            var id = name == "world_snapshot.json" ? SnapshotFileId.World : SnapshotFileId.Terrain; var total = (bytes.Length + ChunkBytes - 1) / ChunkBytes;
            await RelayTransport.SendLockedAsync(session, new SnapshotBegin(name, total, bytes.Length), token);
            for (var i = 0; i < total; i++) { var offset = i * ChunkBytes; await RelayTransport.SendLockedSnapshotChunkAsync(session, id, i, bytes, offset, Math.Min(ChunkBytes, bytes.Length - offset), token); }
            await RelayTransport.SendLockedAsync(session, new SnapshotEnd(name), token);
        }
        static async Task WriteLoop(ClientSession session, CancellationToken token)
        { try { await foreach (var message in session.Outbox.Reader.ReadAllAsync(token)) await RelayTransport.SendLockedAsync(session, message, token); } catch (OperationCanceledException) { } }
    }

    static string BuildRemoteJson(ConcurrentDictionary<string, JsonObject> states, string? exclude)
    { var players = new JsonArray(); foreach (var (pid, state) in states) if (pid != exclude) players.Add(state.DeepClone()); return new JsonObject { ["players"] = players }.ToJsonString(); }
}
