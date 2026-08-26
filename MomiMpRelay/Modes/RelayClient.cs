using System.Text;
using System.Threading.Channels;
using LiteNetLib;
using MomiMpRelay.FileSystem;
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
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var incoming = Channel.CreateUnbounded<RelayPacket>(); var connected = new TaskCompletionSource<NetPeer>(TaskCreationOptions.RunContinuationsAsynchronously);
                var net = new NetManager(new RelayListener((peer, _) => connected.TrySetResult(peer), (_, packet) => incoming.Writer.TryWrite(packet), peer => { incoming.Writer.TryComplete(); connected.TrySetException(new IOException($"Connection to {peer.Address} was closed.")); }));
                using var pollCts = CancellationTokenSource.CreateLinkedTokenSource(ct); Task? poll = null;
                try
                {
                    net.Start(); _reporter.Set("connecting", "join", 0); Console.WriteLine($"[CLIENT] Connecting to {_host}:{_port}…"); net.Connect(_host, _port, RelayProtocol.ConnectionKey);
                    poll = Task.Run(async () => { while (!pollCts.IsCancellationRequested) { net.PollEvents(); try { await Task.Delay(10, pollCts.Token); } catch (OperationCanceledException) { } } }, pollCts.Token);
                    var peer = await connected.Task.WaitAsync(ct); _reporter.Set("connected", "join", 1); Console.WriteLine("[CLIENT] Connected!");
                    using var link = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    if (!requestedSnapshot) { RelayTransport.Send(peer, new SnapshotRequest(), link.Token); requestedSnapshot = true; }
                    var send = SendLoop(peer, link.Token); var receive = ReceiveLoop(incoming.Reader, link.Token); await Task.WhenAny(send, receive); await link.CancelAsync(); try { await Task.WhenAll(send, receive); } catch { }
                }
                finally { await pollCts.CancelAsync(); net.Stop(); if (poll is not null) try { await poll; } catch (OperationCanceledException) { } }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex) { Console.WriteLine($"[CLIENT] {ex.Message}"); }
            if (!ct.IsCancellationRequested) { _reporter.Set("connecting", "join", 0); try { File.Delete(_remotePath); } catch { } try { await Task.Delay(ReconnectDelayMs, ct); } catch (OperationCanceledException) { break; } }
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
            catch (Exception ex) { Console.WriteLine($"[CLIENT] Send: {ex.Message}"); return; }
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
