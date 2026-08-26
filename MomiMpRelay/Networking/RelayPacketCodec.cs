using System.Buffers.Binary;
using System.Text;
using MomiMpRelay.Models;

namespace MomiMpRelay.Networking;

public static class RelayPacketCodec
{
    public static byte[] EncodeJson(IRelayMessage message)
    {
        var jsonBytes = Encoding.UTF8.GetBytes(message.ToJson().ToJsonString());
        var packet = new byte[1 + jsonBytes.Length];
        packet[0] = (byte)RelayPacketKind.Json;
        jsonBytes.CopyTo(packet, 1);
        return packet;
    }

    public static byte[] EncodeSnapshotChunk(byte fileId, int sequence,
        ReadOnlySpan<byte> bytes)
    {
        var packet = new byte[1 + 1 + sizeof(int) + bytes.Length];
        packet[0] = (byte)RelayPacketKind.SnapshotChunk;
        packet[1] = fileId;
        BinaryPrimitives.WriteInt32LittleEndian(packet.AsSpan(2), sequence);
        bytes.CopyTo(packet.AsSpan(2 + sizeof(int)));
        return packet;
    }

    public static bool TryDecode(ReadOnlySpan<byte> packet, out RelayPacket result)
    {
        if (packet.IsEmpty || !Enum.IsDefined((RelayPacketKind)packet[0]))
        {
            result = default;
            return false;
        }

        if (packet[0] == (byte)RelayPacketKind.Json && packet.Length == 1)
        {
            result = default;
            return false;
        }

        if (packet[0] == (byte)RelayPacketKind.SnapshotChunk &&
            (packet.Length < 1 + 1 + sizeof(int) ||
             !IsKnownFileId(packet[1]) ||
             BinaryPrimitives.ReadInt32LittleEndian(packet[2..]) < 0 ||
             packet.Length == 1 + 1 + sizeof(int)))
        {
            result = default;
            return false;
        }

        result = new RelayPacket((RelayPacketKind)packet[0], packet[1..].ToArray());
        return true;
    }

    public static bool TryDecodeSnapshotChunk(RelayPacket packet, out SnapshotChunk result)
    {
        if (packet.Kind != RelayPacketKind.SnapshotChunk || packet.Data.Length < 1 + sizeof(int) ||
            !IsKnownFileId(packet.Data[0]) ||
            BinaryPrimitives.ReadInt32LittleEndian(packet.Data.AsSpan(1)) < 0 ||
            packet.Data.Length == 1 + sizeof(int))
        {
            result = default;
            return false;
        }

        result = new SnapshotChunk(packet.Data[0],
            BinaryPrimitives.ReadInt32LittleEndian(packet.Data.AsSpan(1)),
            packet.Data[(1 + sizeof(int))..]);
        return true;
    }

    static bool IsKnownFileId(byte fileId) => fileId is 1 or 2;
}
