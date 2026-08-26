using System.Text;
using System.Threading.Channels;
using LiteNetLib;
using MomiMpRelay.FileSystem;
using MomiMpRelay.Logging;
using MomiMpRelay.Models;
using MomiMpRelay.Networking;
using MomiMpRelay.Snapshots;
using MomiMpRelay.Status;

namespace MomiMpRelay.Modes;

public sealed class RelayClient
{
    const int PollMs = 50, ReconnectDelayMs = 3000;
    readonly string _host, _mpDir, _outPath, _remotePath; readonly int _port; readonly StatusReporter _reporter;
    public RelayClient(string host, int port, string mpDir, string outPath, string remotePath, StatusReporter reporter)
    { _host = host; _port = port; _mpDir = mpDir; _outPath = outPath; _remotePath = remotePath; _reporter = reporter; }

    public async Task<int> RunAsync(CancellationToken ct)
    {
        bool requestedSnapshot = false;
        var reconnectDelay = TimeSpan.FromSeconds(1);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var incoming = Channel.CreateBounded<RelayPacket>(new BoundedChannelOptions(256) { FullMode = BoundedChannelFullMode.Wait }); var connected = new TaskCompletionSource<NetPeer>(TaskCreationOptions.RunContinuationsAsynchronously);
                var net = new NetManager(new RelayListener((peer, _) => connected.TrySetResult(peer), (peer, packet) => { if (!incoming.Writer.TryWrite(packet)) { RelayLogger.Error($"[CLIENT] Inbox full for {peer.Address}; disconnecting slow peer."); peer.Disconnect(); } }, (peer, disconnect) => { incoming.Writer.TryComplete(); connected.TrySetException(new IOException($"Connection to {peer.Address} was closed: {disconnect.Reason}.")); }));
                using var pollCts = CancellationTokenSource.CreateLinkedTokenSource(ct); Task? poll = null;
                try
                {
                    net.Start(); _reporter.Set("connecting", "join", 0); RelayLogger.Info($"[CLIENT] Connecting to {_host}:{_port}…"); net.Connect(_host, _port, RelayProtocol.ConnectionKey);
                    poll = Task.Run(async () => { while (!pollCts.IsCancellationRequested) { net.PollEvents(); try { await Task.Delay(10, pollCts.Token); } catch (OperationCanceledException) { } } }, pollCts.Token);
                    var peer = await connected.Task.WaitAsync(TimeSpan.FromSeconds(10), ct); _reporter.Set("connected", "join", 1); RelayLogger.Info("[CLIENT] Connected!");
                    reconnectDelay = TimeSpan.FromSeconds(1);
                    using var link = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    if (!requestedSnapshot) { RelayTransport.Send(peer, new SnapshotRequest(), link.Token); requestedSnapshot = true; }
                    var send = SendLoop(peer, link.Token); var receive = ReceiveLoop(incoming.Reader, link.Token); await Task.WhenAny(send, receive); await link.CancelAsync(); try { await Task.WhenAll(send, receive); } catch { }
                }
                finally { await pollCts.CancelAsync(); net.Stop(); if (poll is not null) try { await poll; } catch (OperationCanceledException) { } }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex) { RelayLogger.Error($"[CLIENT] {ex.Message}"); }
            if (!ct.IsCancellationRequested) { _reporter.Set("connecting", "join", 0, $"retrying in {reconnectDelay.TotalSeconds:0}s"); try { File.Delete(_remotePath); } catch { } RelayLogger.Info($"[CLIENT] Reconnecting in {reconnectDelay.TotalSeconds:0}s…"); try { await Task.Delay(reconnectDelay, ct); } catch (OperationCanceledException) { break; } reconnectDelay = TimeSpan.FromSeconds(Math.Min(reconnectDelay.TotalSeconds * 2, 30)); }
        }
        return 0;
    }

    async Task SendLoop(NetPeer peer, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var resync = Path.Combine(_mpDir, "mp_resync"); if (File.Exists(resync)) { try { File.Delete(resync); } catch { } RelayTransport.Send(peer, new SnapshotRequest(), ct); }
                var raw = await RelayFileStore.ReadTextSharedAsync(_outPath, ct); var state = raw is null ? null : PlayerState.Parse(raw); if (state is not null) RelayTransport.Send(peer, state, ct);
                await Task.Delay(PollMs, ct);
            }
            catch (OperationCanceledException) { return; }
            catch (Exception ex) { RelayLogger.Error($"[CLIENT] Send: {ex.Message}"); return; }
        }
    }

    async Task ReceiveLoop(ChannelReader<RelayPacket> messages, CancellationToken ct)
    {
        using var snap = new SnapshotReceiver(_mpDir);
        try
        {
            await foreach (var packet in messages.ReadAllAsync(ct))
            {
                if (packet.Kind == RelayPacketKind.SnapshotChunk) { if (RelayPacketCodec.TryDecodeSnapshotChunk(packet, out var chunk)) await snap.HandleChunkAsync(chunk.FileId, chunk.Sequence, chunk.Data, ct); continue; }
                if (packet.Kind != RelayPacketKind.Json) continue;
                var message = RelayMessageParser.Parse(Encoding.UTF8.GetString(packet.Data));
                if (message is IMpControlMessage control) { await snap.HandleAsync(control, ct); continue; }
                if (message is RelayStateUpdate update) await File.WriteAllTextAsync(_remotePath, update.ToJson().ToJsonString(), ct);
            }
        }
        catch (OperationCanceledException) { }
    }
}
