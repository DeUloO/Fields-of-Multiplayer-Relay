using System.Text;
using MomiMpRelay.Models;
using MomiMpRelay.Networking;

namespace MomiMpRelay.Tests;

public sealed class RelayPacketCodecTests
{
    [Fact]
    public void EncodeJsonAndDecodeRoundTripsMessage()
    {
        var message = new PlayerState { PlayerId = "p1" };

        var encoded = RelayPacketCodec.EncodeJson(JsonIdentifier.player_id, message);

        Assert.True(RelayPacketCodec.TryDecode(encoded, out var packet));
        var kind = Assert.IsType<RelayPacketKind.Json>(packet.Kind);
        Assert.Equal(JsonIdentifier.player_id, kind.Identifier);
        Assert.Equal("{\"player_id\":\"p1\"}", Encoding.UTF8.GetString(packet.Data));
    }

    [Fact]
    public void EncodeSnapshotChunkAndDecodeRoundTripsMetadataAndBytes()
    {
        var source = new byte[] { 1, 2, 3, 255 };

        var encoded = RelayPacketCodec.EncodeSnapshotChunk(SnapshotFileId.Terrain, 17, source);

        Assert.True(RelayPacketCodec.TryDecode(encoded, out var packet));
        Assert.True(RelayPacketCodec.TryDecodeSnapshotChunk(packet, out var chunk));
        Assert.Equal(SnapshotFileId.Terrain, chunk.FileId);
        Assert.Equal(17, chunk.Sequence);
        Assert.Equal(source, chunk.Data);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(255)]
    public void DecodeRejectsUnknownPacketKind(byte kind)
    {
        Assert.False(RelayPacketCodec.TryDecode(new byte[] { kind }, out _));
    }

    [Fact]
    public void DecodeRejectsEmptyPacket()
    {
        Assert.False(RelayPacketCodec.TryDecode([], out _));
    }

    [Theory]
    [InlineData(new byte[] { 2 })]
    [InlineData(new byte[] { 2, 1 })]
    [InlineData(new byte[] { 2, 1, 0, 0, 0, 0 })]
    [InlineData(new byte[] { 2, 9, 0, 0, 0, 1, 4 })]
    [InlineData(new byte[] { 2, 1, 255, 255, 255, 255, 4 })]
    public void DecodeRejectsMalformedSnapshotPackets(byte[] packet)
    {
        Assert.False(RelayPacketCodec.TryDecode(packet, out _));
    }

    [Fact]
    public void DecodeSnapshotChunkReturnsTypedMetadata()
    {
        var encoded = RelayPacketCodec.EncodeSnapshotChunk(SnapshotFileId.Terrain, 17, new byte[] { 7, 8 });
        Assert.True(RelayPacketCodec.TryDecode(encoded, out var packet));

        Assert.True(RelayPacketCodec.TryDecodeSnapshotChunk(packet, out var chunk));
        Assert.Equal(SnapshotFileId.Terrain, chunk.FileId);
        Assert.Equal(17, chunk.Sequence);
        Assert.Equal(new byte[] { 7, 8 }, chunk.Data);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(int.MaxValue)]
    public void SnapshotSequenceBoundariesRoundTrip(int sequence)
    {
        var encoded = RelayPacketCodec.EncodeSnapshotChunk(SnapshotFileId.World, sequence, new byte[] { 42 });

        Assert.True(RelayPacketCodec.TryDecode(encoded, out var packet));
        Assert.True(RelayPacketCodec.TryDecodeSnapshotChunk(packet, out var chunk));
        Assert.Equal(sequence, chunk.Sequence);
    }

    [Fact]
    public void LargeCodecPayloadRoundTripsWithoutJsonEncoding()
    {
        var source = Enumerable.Range(0, 20_000).Select(value => (byte)(value % 251)).ToArray();

        var encoded = RelayPacketCodec.EncodeSnapshotChunk(SnapshotFileId.Terrain, 3, source);

        Assert.True(RelayPacketCodec.TryDecode(encoded, out var packet));
        Assert.True(RelayPacketCodec.TryDecodeSnapshotChunk(packet, out var chunk));
        Assert.Equal(source, chunk.Data);
    }
}
