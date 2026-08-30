using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using LiteNetLib;
using MomiMpRelay.FileSystem;
using MomiMpRelay.Ledger;
using MomiMpRelay.Logging;
using MomiMpRelay.Models;
using MomiMpRelay.Networking;
using MomiMpRelay.Status;

namespace MomiMpRelay.Modes;

public sealed class RelayHost
{
    const int PollMs = 50;
    const int ChunkBytes = 900;
    const int SnapshotTimeoutMs = 20_000;

    readonly int _port;
    readonly string _mpDir;
    readonly string _outPath;
    readonly string _remotePath;
    readonly StatusReporter _reporter;

    public RelayHost(int port, string mpDir, string outPath, string remotePath, StatusReporter reporter)
    {
        _port = port;
        _mpDir = mpDir;
        _outPath = outPath;
        _remotePath = remotePath;
        _reporter = reporter;
    }

    public async Task<int> RunAsync(CancellationToken ct)
    {
        var states = new ConcurrentDictionary<string, PlayerState>();
        var sessions = new ConcurrentDictionary<ClientSession, byte>();
        using var writeLock = new SemaphoreSlim(1, 1);
        using var snapshotLock = new SemaphoreSlim(1, 1);
        using var ledger = new MutationLedger(_mpDir);
        var outboxDir = Path.Combine(_mpDir, "outbox");
        var inboxDir = Path.Combine(_mpDir, "inbox");
        string? hostPid = null;
        void PushToAll()
        {
            foreach (var (session, _) in sessions)
                if (session.PlayerId is { } pid)
                    if (!session.Push(new RelayPacketKind.Json(JsonIdentifier.player_id), new RelayStateUpdate(states, pid)))
                        RelayLogger.Error($"[HOST] Outbox full for {session.Peer.Address}; state update dropped.");
        }
        void Refresh() => _reporter.Set("listening", "host", sessions.Count);
        var net = new NetManager(new RelayListener(
            (peer, address) =>
            {
                var session = new ClientSession(peer);
                sessions.TryAdd(session, 0);
                Refresh();
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
                var session = sessions.Keys.FirstOrDefault(s => s.Peer == peer);
                if (session is null)
                    return;
                session.Inbox.Writer.TryComplete();
                if (session.PlayerId is { } pid)
                    states.TryRemove(pid, out _);
                sessions.TryRemove(session, out _);
                session.Dispose();
                Refresh();
                RelayLogger.Info($"[HOST] Disconnected {peer.Address}: {disconnect.Reason}");
            }));
        net.Start(_port);
        using var pollCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var pollTask = Task.Run(async () =>
        {
            while (!pollCts.IsCancellationRequested)
            {
                net.PollEvents();
                try
                {
                    await Task.Delay(10, pollCts.Token);
                }
                catch (OperationCanceledException)
                {
                }
            }
        }, pollCts.Token);
        _reporter.Set("listening", "host", 0);
        RelayLogger.Info($"[HOST] Listening on :{_port}");
        try
        {
            byte[]? lastHash = null;
            while (!ct.IsCancellationRequested)
            {
                var hash = await RelayFileStore.GetSha256Async(_outPath, ct);
                if (!hash.SequenceEqual(lastHash))
                {
                    var raw = await RelayFileStore.ReadTextSharedAsync(_outPath, ct);
                    lastHash = hash;
                    var state = JsonSerializer.Deserialize<PlayerState>(raw ?? string.Empty);
                    if (state is not null)
                    {
                        try
                        {
                            foreach (var ev in state.Events)
                            {
                                MutationValidator.EnsureValid(ev);
                            }
                        }
                        catch (JsonException ex)
                        {
                            RelayLogger.Error($"[HOST] Rejected invalid local mutation state: {ex.Message}");
                            continue;
                        }
                        hostPid = state.PlayerId;
                        states[state.PlayerId] = state;
                        await RelayFileStore.WriteRemoteAsync(_remotePath, JsonSerializer.Serialize(new RelayStateUpdate(states, hostPid)), writeLock, ct);
                        PushToAll();
                    }
                }

                // Self round trip only: the host's own player, via its own local files and ledger.
                // Remote joiners are not yet fed into this ledger (deferred to the network-forwarding step).
                MutationOutboxIngestor.Ingest(outboxDir, ledger);
                if (hostPid is not null)
                {
                    var batch = MutationInboxMaterializer.BuildBatch(ledger, hostPid, hostPid, maxEvents: 500);
                    if (batch is not null)
                        MutationInboxPublisher.PublishAtomic(inboxDir, batch);
                }

                await Task.Delay(PollMs, ct);
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            await pollCts.CancelAsync();
            try
            {
                await pollTask;
            }
            catch (OperationCanceledException)
            {
            }

            net.Stop();
            foreach (var (session, _) in sessions)
            {
                session.Dispose();
            }
        }
        return 0;

        async Task ReadLoop(ClientSession session, CancellationToken token)
        {
            try
            {
                await foreach (var packet in session.Inbox.Reader.ReadAllAsync(token))
                {
                    if (packet.Kind.Type != RelayPacketKindType.Json)
                        continue;
                    var message = RelayMessageParser.Parse(packet);
                    if (message is SnapshotRequest)
                    {
                        await SendSnapshot(session, token);
                        continue;
                    }
                    if (message is not PlayerState state)
                        continue;
                    try
                    {
                        foreach (var ev in state.Events)
                            MutationValidator.EnsureValid(ev);
                    }
                    catch (JsonException ex)
                    {
                        RelayLogger.Error($"[HOST] Rejected invalid mutation state from {session.Peer.Address}: {ex.Message}");
                        continue;
                    }
                    session.PlayerId = state.PlayerId;
                    states[state.PlayerId] = state;
                    if (hostPid is not null)
                        await RelayFileStore.WriteRemoteAsync(_remotePath, JsonSerializer.Serialize(new RelayStateUpdate(states, hostPid)), writeLock, token);
                    PushToAll();
                }
            }
            catch (OperationCanceledException) { }
        }
        async Task SendSnapshot(ClientSession session, CancellationToken token)
        {
            byte[]? world;
            byte[]? terrain;
            await snapshotLock.WaitAsync(token);
            try
            {
                var request = Path.Combine(_mpDir, "mp_snap_request");
                var ready = Path.Combine(_mpDir, "mp_snap_ready");
                try
                {
                    File.Delete(ready);
                }
                catch { }
                await File.WriteAllTextAsync(request, "{}", token);
                var start = DateTime.UtcNow;
                while (!File.Exists(ready))
                {
                    if ((DateTime.UtcNow - start).TotalMilliseconds > SnapshotTimeoutMs)
                        return;
                    await Task.Delay(100, token);
                }
                try
                {
                    File.Delete(ready);
                }
                catch { }
                world = await RelayFileStore.ReadBytesSharedAsync(Path.Combine(_mpDir, "world_snapshot.json"), token);
                terrain = await RelayFileStore.ReadBytesSharedAsync(Path.Combine(_mpDir, "world_farm_terrain.bin"), token);
            }
            finally { snapshotLock.Release(); }
            if (world is null)
                return;
            await SendFile(session, "world_snapshot.json", world, token);
            if (terrain is not null)
                await SendFile(session, "world_farm_terrain.bin", terrain, token);
            await RelayTransport.SendLockedAsync(session, JsonIdentifier.snap_done, new SnapshotDone(), token);
        }
        async Task SendFile(ClientSession session, string name, byte[] bytes, CancellationToken token)
        {
            var id = name == "world_snapshot.json" ? SnapshotFileId.World : SnapshotFileId.Terrain;
            var total = (bytes.Length + ChunkBytes - 1) / ChunkBytes;
            await RelayTransport.SendLockedAsync(session, JsonIdentifier.snap_begin, new SnapshotBegin(name, total, bytes.Length), token);
            for (var i = 0; i < total; i++)
            {
                var offset = i * ChunkBytes;
                await RelayTransport.SendLockedSnapshotChunkAsync(session, id, i, bytes, offset, Math.Min(ChunkBytes, bytes.Length - offset), token);
            }
            await RelayTransport.SendLockedAsync(session, JsonIdentifier.snap_end, new SnapshotEnd(name), token);
        }
        static async Task WriteLoop(ClientSession session, CancellationToken token)
        {
            try
            {
                await foreach (var message in session.Outbox.Reader.ReadAllAsync(token))
                {
                    await RelayTransport.SendLockedAsync(session, ((RelayPacketKind.Json)message.Kind).Identifier, message.Packet, token);
                }
            }
            catch (OperationCanceledException) { }
        }
    }
}
