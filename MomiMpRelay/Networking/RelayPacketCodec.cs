using System.Buffers.Binary;
using System.Text;
using System.Text.Json;
using MomiMpRelay.Models;

namespace MomiMpRelay.Networking;

public static class RelayPacketCodec
{
    public static byte[] EncodeJson(JsonIdentifier identifier, IRelayPacket message)
    {
        var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(message);
        var packet = new byte[1 + 1 + jsonBytes.Length];
        packet[0] = (byte)RelayPacketKindType.Json;
        packet[1] = (byte)identifier;
        jsonBytes.CopyTo(packet, 2);
        return packet;
    }

    public static byte[] EncodeSnapshotChunk(SnapshotFileId fileId, int sequence,
        ReadOnlySpan<byte> bytes)
    {
        var packet = new byte[1 + 1 + sizeof(int) + bytes.Length];
        packet[0] = (byte)RelayPacketKindType.SnapshotChunk;
        packet[1] = (byte)fileId;
        BinaryPrimitives.WriteInt32LittleEndian(packet.AsSpan(2), sequence);
        bytes.CopyTo(packet.AsSpan(2 + sizeof(int)));
        return packet;
    }

    public static bool TryDecode(ReadOnlySpan<byte> packet, out RelayPacket result)
    {
        if (packet.IsEmpty || !Enum.IsDefined((RelayPacketKindType)packet[0]))
        {
            result = default;
            return false;
        }

        if (packet[0] == (byte)RelayPacketKindType.Json && packet.Length == 1)
        {
            result = default;
            return false;
        }

        if (packet[0] == (byte)RelayPacketKindType.SnapshotChunk &&
            (packet.Length < 1 + 1 + sizeof(int) ||
             !IsKnownFileId(packet[1]) ||
             BinaryPrimitives.ReadInt32LittleEndian(packet[2..]) < 0 ||
             packet.Length == 1 + 1 + sizeof(int)))
        {
            result = default;
            return false;
        }
        var kindType = (RelayPacketKindType)packet[0];
        var identifier = packet[1];
        switch (kindType)
        {
            case RelayPacketKindType.Json: 
                result = new RelayPacket(new RelayPacketKind.Json((JsonIdentifier)identifier), packet[2..].ToArray());
                return true;
            case RelayPacketKindType.SnapshotChunk:
                int sequence = BinaryPrimitives.ReadInt32LittleEndian(packet[2..]);
                result = new RelayPacket(new RelayPacketKind.SnapshotChunk((SnapshotFileId)identifier, sequence), packet[(1 + sizeof(int))..].ToArray());
                return true;
            default:
                result = default;
                return false;
        }        
    }

    public static bool TryDecodeSnapshotChunk(RelayPacket packet, out SnapshotChunk result)
    {
        if (packet.Kind.Type != RelayPacketKindType.SnapshotChunk || packet.Data.Length < 1 + sizeof(int) ||
            !IsKnownFileId(packet.Data[0]) ||
            BinaryPrimitives.ReadInt32LittleEndian(packet.Data.AsSpan(1)) < 0 ||
            packet.Data.Length == 1 + sizeof(int))
        {
            result = default;
            return false;
        }

        result = new SnapshotChunk((SnapshotFileId)packet.Data[0],
            BinaryPrimitives.ReadInt32LittleEndian(packet.Data.AsSpan(1)),
            packet.Data[(1 + sizeof(int))..]);
        return true;
    }

    static bool IsKnownFileId(byte fileId) => Enum.IsDefined((SnapshotFileId)fileId);
}
