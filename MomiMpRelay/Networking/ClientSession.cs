using System.Threading.Channels;
using LiteNetLib;
using MomiMpRelay.Models;

namespace MomiMpRelay.Networking;

sealed class ClientSession
{
    public readonly NetPeer Peer;
    public volatile string? PlayerId;

    public readonly Channel<IRelayMessage> Outbox = Channel.CreateBounded<IRelayMessage>(
        new BoundedChannelOptions(2) { FullMode = BoundedChannelFullMode.DropOldest });

    public readonly Channel<RelayPacket> Inbox = Channel.CreateUnbounded<RelayPacket>();
    public readonly SemaphoreSlim WriteLock = new(1, 1);

    public ClientSession(NetPeer peer) => Peer = peer;

    public void Push(IRelayMessage message) => Outbox.Writer.TryWrite(message);
}
