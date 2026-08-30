using LiteNetLib;
using MomiMpRelay.Models;

namespace MomiMpRelay.Networking;

static class RelayTransport
{
    public static void SendJson(NetPeer peer, JsonIdentifier identifier, IRelayPacket message, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        peer.Send(RelayPacketCodec.EncodeJson(identifier, message), DeliveryMethod.ReliableOrdered);
    }

    public static void SendSnapshotChunk(NetPeer peer, SnapshotFileId fileId, int sequence,
        byte[] bytes, int offset, int count, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        peer.Send(RelayPacketCodec.EncodeSnapshotChunk(fileId, sequence,
            bytes.AsSpan(offset, count)), DeliveryMethod.ReliableOrdered);
    }

    public static async Task SendLockedAsync(ClientSession session, JsonIdentifier identifier,
        IRelayPacket message, CancellationToken ct)
    {
        await session.WriteLock.WaitAsync(ct);
        try
        {
            SendJson(session.Peer, identifier, message, ct);
        }
        finally { session.WriteLock.Release(); }
    }

    public static async Task SendLockedSnapshotChunkAsync(ClientSession session,
        SnapshotFileId fileId, int sequence, byte[] bytes, int offset, int count,
        CancellationToken ct)
    {
        await session.WriteLock.WaitAsync(ct);
        try
        {
            SendSnapshotChunk(session.Peer, fileId, sequence, bytes, offset, count, ct);
        }
        finally { session.WriteLock.Release(); }
    }
}
