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
}
