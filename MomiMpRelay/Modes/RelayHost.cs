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
    const string RepairRequestFileName = "mp_repair_request.json";

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
        var sessions = new ConcurrentDictionary<NetPeer, ClientSession>();
        using var writeLock = new SemaphoreSlim(1, 1);
        using var snapshotLock = new SemaphoreSlim(1, 1);
        using var ledger = new MutationLedger(_mpDir);
        var outboxDir = Path.Combine(_mpDir, "outbox");
        var inboxDir = Path.Combine(_mpDir, "inbox");
        string? hostPid = null;
        void PushToAll()
        {
            // GML already ignores updates about its own player, so one shared update serves every recipient.
            var update = new RelayStateUpdate(states, exclude: null);
            foreach (var session in sessions.Values)
                if (session.PlayerId is not null)
                    if (!session.Push(new RelayPacketKind.Json(JsonIdentifier.player_id), update))
                        RelayLogger.Error($"[HOST] Outbox full for {session.Peer.Address}; state update dropped.");
        }
        void Refresh() => _reporter.Set("listening", "host", sessions.Count);
        var net = new NetManager(new RelayListener(
            (peer, address) =>
            {
                var session = new ClientSession(peer);
                sessions.TryAdd(peer, session);
                Refresh();
                RelayLogger.Info($"[HOST] + {address}");
                _ = Task.Run(() => WriteLoop(session, ct), ct);
                _ = Task.Run(() => ReadLoop(session, ct), ct);
            },
            (peer, packet) =>
            {
                if (sessions.TryGetValue(peer, out var session) && !session.Inbox.Writer.TryWrite(packet))
                {
                    RelayLogger.Error($"[HOST] Inbox full for {peer.Address}; disconnecting slow peer.");
                    peer.Disconnect();
                }
            },
            (peer, disconnect) =>
            {
                if (!sessions.TryRemove(peer, out var session))
                    return;
                session.Inbox.Writer.TryComplete();
                if (session.PlayerId is { } pid)
                    states.TryRemove(pid, out _);
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
        using var pruneLedgerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        const int PruneLedgerIntervalMs = 10000;
        var pruneLedgerTask = Task.Run(async () =>
        {
            while (!pruneLedgerCts.IsCancellationRequested)
            {
                try
                {
                    if (string.IsNullOrWhiteSpace(hostPid))
                    {
                        await Task.Delay(PruneLedgerIntervalMs, pruneLedgerCts.Token);
                        continue;
                    }
                    ledger.PruneSessionProducerEvents(hostPid);
                    await Task.Delay(PruneLedgerIntervalMs, pruneLedgerCts.Token);
                }
                catch (OperationCanceledException)
                {
                }
            }
        }, pruneLedgerCts.Token);
        _reporter.Set("listening", "host", 0);
        RelayLogger.Info($"[HOST] Listening on :{_port}");
        try
        {
            string? lastRaw = null;
            long hostLastRelaySeq = 0L;
            while (!ct.IsCancellationRequested)
            {
                var raw = await RelayFileStore.ReadTextSharedAsync(_outPath, ct);
                if (raw is not null && raw != lastRaw)
                {
                    lastRaw = raw;
                    var state = JsonSerializer.Deserialize<PlayerState>(raw);
                    if (state is not null)
                    {
                        hostPid = state.PlayerId;
                        states[state.PlayerId] = state;
                        await RelayFileStore.WriteRemoteAsync(_remotePath, JsonSerializer.Serialize(new RelayStateUpdate(states, hostPid)), writeLock, ct);
                        PushToAll();
                    }
                }

                MutationOutboxIngestor.Ingest(outboxDir, ledger);
                if (hostPid is not null)
                {
                    var repairRequestPath = Path.Combine(_mpDir, RepairRequestFileName);
                    if (File.Exists(repairRequestPath))
                    {
                        try
                        {
                            var repairRaw = await File.ReadAllTextAsync(repairRequestPath, ct);
                            var request = JsonSerializer.Deserialize<RepairRequest>(repairRaw);
                            if (request is not null)
                            {
                                ledger.RecordClientCursor(hostPid, request.PlayerId, request.ReportedCursor);
                                ledger.RecordRepairRequest(hostPid, request.PlayerId, request.Reason, request.ReportedCursor);
                                hostLastRelaySeq = -1;
                                RelayLogger.Info($"[HOST] Repair requested locally by {request.PlayerId} ({request.Reason}) at cursor {request.ReportedCursor}; forcing resync.");
                            }
                        }
                        catch (Exception ex)
                        {
                            RelayLogger.Error($"[HOST] Failed to process local repair request: {ex.Message}");
                        }
                        finally
                        {
                            try { File.Delete(repairRequestPath); } catch { }
                        }
                    }

                    if (ledger.GetHeadRelaySeq(hostPid) != hostLastRelaySeq)
                    {
                        var batch = MutationInboxMaterializer.BuildBatch(ledger, hostPid, hostPid, maxEvents: 500);
                        if (batch is not null)
                        {
                            MutationInboxPublisher.PublishAtomic(inboxDir, batch);
                            hostLastRelaySeq = ledger.GetHeadRelaySeq(hostPid);
                        }
                    }

                    foreach (var session in sessions.Values)
                    {
                        if (string.IsNullOrWhiteSpace(session.PlayerId))
                            continue;
                        if (ledger.GetHeadRelaySeq(hostPid) == session.LastPublishedRelaySeq)
                            continue;
                        var batch = MutationInboxMaterializer.BuildBatch(ledger, hostPid, session.PlayerId, maxEvents: 500);
                        if (batch is not null)
                        {
                            session.LastPublishedRelaySeq = ledger.GetHeadRelaySeq(hostPid);
                            session.Push(new RelayPacketKind.Json(JsonIdentifier.mutation_batch_download), new MutationBatchDownload(batch));
                        }

                    }

                    var head = ledger.GetHeadRelaySeq(hostPid);
                    var clientLag = new Dictionary<string, long> { [hostPid] = head - ledger.GetClientCursor(hostPid, hostPid) };
                    foreach (var session in sessions.Values)
                        if (session.PlayerId is { } pid)
                            clientLag[pid] = head - ledger.GetClientCursor(hostPid, pid);
                    var outboxPending = Directory.Exists(outboxDir) ? Directory.GetFiles(outboxDir, "segment-*.json").Length : 0;
                    var inboxPending = File.Exists(Path.Combine(inboxDir, MutationInboxPublisher.PendingBatchFileName)) ? 1 : 0;
                    _reporter.SetMutationDiagnostics(head, clientLag, outboxPending, inboxPending);
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
            foreach (var session in sessions.Values)
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

                    switch (message)
                    {
                        case SnapshotRequest:
                            await SendSnapshot(session, token);
                            continue;
                        case PlayerState state:
                            await HandlePlayerState(state, states, session, hostPid, _remotePath, writeLock, PushToAll, token);
                            continue;
                        case MutationBatchUpload batch:
                            await HandleMutationBatchUpload(batch, ledger, session, token);
                            continue;
                        case RepairRequest request:
                            HandleRepairRequest(request, ledger, hostPid, session);
                            continue;
                    }

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

        static async Task HandlePlayerState(PlayerState state, ConcurrentDictionary<string, PlayerState> states, ClientSession session, string? hostPid, string _remotePath, SemaphoreSlim writeLock, Action PushToAll, CancellationToken token)
        {
            session.PlayerId = state.PlayerId;
            states[state.PlayerId] = state;
            if (hostPid is not null)
                await RelayFileStore.WriteRemoteAsync(_remotePath, JsonSerializer.Serialize(new RelayStateUpdate(states, hostPid)), writeLock, token);
            PushToAll();
        }

        static async Task HandleMutationBatchUpload(MutationBatchUpload batch, MutationLedger ledger, ClientSession session, CancellationToken token)
        {
            try
            {
                var summary = MutationOutboxIngestor.AcceptAll(ledger, batch.Entries);
                if (summary.Last is not { } last)
                    return;
                var ack = new MutationBatchUploadAck(new MutationOutboxAck(last.Protocol, last.PlayerId, last.ClientEpoch, summary.MaxClientSeq, ledger.GetHeadRelaySeq(last.SessionId)));
                await RelayTransport.SendLockedAsync(session, JsonIdentifier.mutation_batch_upload_ack, ack, token);
            }
            catch (Exception ex)
            {
                RelayLogger.Error($"[HOST] Failed to handle mutation batch upload: {ex.Message}");
            }
        }

        static void HandleRepairRequest(RepairRequest request, MutationLedger ledger, string? hostPid, ClientSession session)
        {
            if (string.IsNullOrWhiteSpace(hostPid))
                return;
            ledger.RecordClientCursor(hostPid, request.PlayerId, request.ReportedCursor);
            ledger.RecordRepairRequest(hostPid, request.PlayerId, request.Reason, request.ReportedCursor);
            session.LastPublishedRelaySeq = -1;
            RelayLogger.Info($"[HOST] Repair requested by {request.PlayerId} ({request.Reason}) at cursor {request.ReportedCursor}; forcing resync.");
        }
    }
}
