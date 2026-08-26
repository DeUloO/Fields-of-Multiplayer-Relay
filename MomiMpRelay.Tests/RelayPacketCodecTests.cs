using System.Text;
using System.Text.Json.Nodes;
using Moq;
using MomiMpRelay.Models;
using MomiMpRelay.Networking;

namespace MomiMpRelay.Tests;

public sealed class RelayPacketCodecTests
{
    [Fact]
    public void EncodeJsonAndDecodeRoundTripsMessage()
    {
        var message = new Mock<IRelayMessage>();
        message.SetupGet(value => value.Identifier).Returns("test");
        message.Setup(value => value.ToJson()).Returns(new JsonObject { ["value"] = 42 });

        var encoded = RelayPacketCodec.EncodeJson(message.Object);

        Assert.True(RelayPacketCodec.TryDecode(encoded, out var packet));
        Assert.Equal(RelayPacketKind.Json, packet.Kind);
        Assert.Equal("{\"value\":42}", Encoding.UTF8.GetString(packet.Data));
    }

    [Fact]
    public void EncodeSnapshotChunkAndDecodeRoundTripsMetadataAndBytes()
    {
        var source = new byte[] { 1, 2, 3, 255 };

        var encoded = RelayPacketCodec.EncodeSnapshotChunk(2, 17, source);

        Assert.True(RelayPacketCodec.TryDecode(encoded, out var packet));
        Assert.Equal(RelayPacketKind.SnapshotChunk, packet.Kind);
        Assert.Equal(2, packet.Data[0]);
        Assert.Equal(17, BitConverter.ToInt32(packet.Data, 1));
        Assert.Equal(source, packet.Data[5..]);
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
        var packet = new RelayPacket(RelayPacketKind.SnapshotChunk,
            new byte[] { 2, 17, 0, 0, 0, 7, 8 });

        Assert.True(RelayPacketCodec.TryDecodeSnapshotChunk(packet, out var chunk));
        Assert.Equal(2, chunk.FileId);
        Assert.Equal(17, chunk.Sequence);
        Assert.Equal(new byte[] { 7, 8 }, chunk.Data);
    }
}
