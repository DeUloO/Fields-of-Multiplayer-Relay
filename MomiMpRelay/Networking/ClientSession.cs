using System.Threading.Channels;
using LiteNetLib;
using MomiMpRelay.Models;

namespace MomiMpRelay.Networking;

sealed class ClientSession : IDisposable
{
    public readonly NetPeer Peer;
    public volatile string? PlayerId;
    public long LastPublishedRelaySeq = 0L;

    public readonly Channel<(RelayPacketKind Kind, IRelayPacket Packet)> Outbox = Channel.CreateBounded<(RelayPacketKind Kind, IRelayPacket Packet)>(
        new BoundedChannelOptions(2) { FullMode = BoundedChannelFullMode.DropOldest });

    public readonly Channel<RelayPacket> Inbox = Channel.CreateBounded<RelayPacket>(
        new BoundedChannelOptions(256) { FullMode = BoundedChannelFullMode.Wait });
    public readonly SemaphoreSlim WriteLock = new(1, 1);

    public ClientSession(NetPeer peer) => Peer = peer;

    public bool Push(RelayPacketKind kind, IRelayPacket message) => Outbox.Writer.TryWrite((kind, message));

    public void Dispose()
    {
        Inbox.Writer.TryComplete();
        Outbox.Writer.TryComplete();
        WriteLock.Dispose();
    }
}
