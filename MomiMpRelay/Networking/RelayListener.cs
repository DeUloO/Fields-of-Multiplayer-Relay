using System.Net;
using LiteNetLib;
using MomiMpRelay.Models;

namespace MomiMpRelay.Networking;

sealed class RelayListener : INetEventListener
{
    readonly Action<NetPeer, string> _connected;
    readonly Action<NetPeer, RelayPacket> _received;
    readonly Action<NetPeer> _disconnected;

    public RelayListener(
        Action<NetPeer, string> connected,
        Action<NetPeer, RelayPacket> received,
        Action<NetPeer> disconnected)
    {
        _connected = connected;
        _received = received;
        _disconnected = disconnected;
    }

    public void OnPeerConnected(NetPeer peer) => _connected(peer, peer.Address.ToString());
    public void OnPeerDisconnected(NetPeer peer, DisconnectInfo disconnectInfo) => _disconnected(peer);

    public void OnNetworkReceive(NetPeer peer, NetPacketReader reader, byte channelNumber,
        DeliveryMethod deliveryMethod)
    {
        try
        {
            var data = reader.GetRemainingBytes();
            if (RelayPacketCodec.TryDecode(data, out var packet))
                _received(peer, packet);
        }
        finally { reader.Recycle(); }
    }

    public void OnNetworkError(IPEndPoint endPoint, System.Net.Sockets.SocketError socketError) { }
    public void OnNetworkReceiveUnconnected(IPEndPoint remoteEndPoint, NetPacketReader reader,
        UnconnectedMessageType messageType) => reader.Recycle();
    public void OnNetworkLatencyUpdate(NetPeer peer, int latency) { }
    public void OnConnectionRequest(ConnectionRequest request) =>
        request.AcceptIfKey(RelayProtocol.ConnectionKey);
}
