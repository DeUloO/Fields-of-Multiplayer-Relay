using System.Buffers.Binary;
using System.Text;
using System.Text.Json;
using MomiMpRelay.Models;

namespace MomiMpRelay.Networking;

public static class RelayPacketCodec
{
    public static byte[] EncodeJson(JsonIdentifier identifier, IRelayPacket message)
    {
        // Serialize by runtime type; message's static type is the empty IRelayPacket marker interface.
        var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(message, message.GetType());
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
                result = new RelayPacket(new RelayPacketKind.SnapshotChunk((SnapshotFileId)identifier, sequence), packet[(2 + sizeof(int))..].ToArray());
                return true;
            default:
                result = default;
                return false;
        }        
    }

    public static bool TryDecodeSnapshotChunk(RelayPacket packet, out SnapshotChunk result)
    {
        if (packet.Kind is not RelayPacketKind.SnapshotChunk chunkKind)
        {
            result = default;
            return false;
        }

        result = new SnapshotChunk(chunkKind.FileId, chunkKind.Sequence, packet.Data);
        return true;
    }

    static bool IsKnownFileId(byte fileId) => Enum.IsDefined((SnapshotFileId)fileId);
}
