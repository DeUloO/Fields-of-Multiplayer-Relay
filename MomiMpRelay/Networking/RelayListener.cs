using System.Net;
using LiteNetLib;
using MomiMpRelay.Logging;
using MomiMpRelay.Models;

namespace MomiMpRelay.Networking;

sealed class RelayListener : INetEventListener
{
    readonly Action<NetPeer, string> _connected;
    readonly Action<NetPeer, RelayPacket> _received;
    readonly Action<NetPeer, DisconnectInfo> _disconnected;
    readonly Action<IPEndPoint, System.Net.Sockets.SocketError> _networkError;

    public RelayListener(
        Action<NetPeer, string> connected,
        Action<NetPeer, RelayPacket> received,
        Action<NetPeer, DisconnectInfo> disconnected,
        Action<IPEndPoint, System.Net.Sockets.SocketError>? networkError = null)
    {
        _connected = connected;
        _received = received;
        _disconnected = disconnected;
        _networkError = networkError ?? ((endpoint, error) =>
            RelayLogger.Error($"[NET] {endpoint}: {error}"));
    }

    public void OnPeerConnected(NetPeer peer) => _connected(peer, peer.Address.ToString());
    public void OnPeerDisconnected(NetPeer peer, DisconnectInfo disconnectInfo) => _disconnected(peer, disconnectInfo);

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

    public void OnNetworkError(IPEndPoint endPoint, System.Net.Sockets.SocketError socketError) =>
        _networkError(endPoint, socketError);
    public void OnNetworkReceiveUnconnected(IPEndPoint remoteEndPoint, NetPacketReader reader,
        UnconnectedMessageType messageType) => reader.Recycle();
    public void OnNetworkLatencyUpdate(NetPeer peer, int latency)
    {
    }
    public void OnConnectionRequest(ConnectionRequest request) =>
        request.AcceptIfKey(RelayProtocol.ConnectionKey);
}
