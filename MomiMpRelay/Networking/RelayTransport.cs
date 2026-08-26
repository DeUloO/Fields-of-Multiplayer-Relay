using LiteNetLib;
using MomiMpRelay.Models;

namespace MomiMpRelay.Networking;

static class RelayTransport
{
    public static void Send(NetPeer peer, IRelayMessage message, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        peer.Send(RelayPacketCodec.EncodeJson(message), DeliveryMethod.ReliableOrdered);
    }

    public static void SendSnapshotChunk(NetPeer peer, byte fileId, int sequence,
        byte[] bytes, int offset, int count, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        peer.Send(RelayPacketCodec.EncodeSnapshotChunk(fileId, sequence,
            bytes.AsSpan(offset, count)), DeliveryMethod.ReliableOrdered);
    }

    public static async Task SendLockedAsync(ClientSession session,
        IRelayMessage message, CancellationToken ct)
    {
        await session.WriteLock.WaitAsync(ct);
        try { Send(session.Peer, message, ct); }
        finally { session.WriteLock.Release(); }
    }

    public static async Task SendLockedSnapshotChunkAsync(ClientSession session,
        byte fileId, int sequence, byte[] bytes, int offset, int count,
        CancellationToken ct)
    {
        await session.WriteLock.WaitAsync(ct);
        try { SendSnapshotChunk(session.Peer, fileId, sequence, bytes, offset, count, ct); }
        finally { session.WriteLock.Release(); }
    }
}
