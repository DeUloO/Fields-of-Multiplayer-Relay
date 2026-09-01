using System.Text;
using System.Text.Json;
using MomiMpRelay.Models;

namespace MomiMpRelay.Tests;

public sealed class RelayMessageTests
{
    [Fact]
    public void RelayControlReadsFractionalPortFromGameJson()
    {
        const string json = "{\"ip\":\"127.0.0.1\",\"mode\":\"host\",\"port\":7777.0,\"seq\":2}";

        var control = JsonSerializer.Deserialize<RelayControl>(json);

        Assert.NotNull(control);
        Assert.Equal("127.0.0.1", control.Ip);
        Assert.Equal("host", control.Mode);
        Assert.Equal(7777, control.Port);
        Assert.Equal(2, control.Seq);
    }

    [Fact]
    public void RelayControlUsesDefaultsForMissingFields()
    {
        var control = JsonSerializer.Deserialize<RelayControl>("{}");

        Assert.NotNull(control);
        Assert.Equal("off", control.Mode);
        Assert.Equal("127.0.0.1", control.Ip);
        Assert.Equal(0, control.Port);
        Assert.Equal(0, control.Seq);
    }

    [Fact]
    public void MessageParserDispatchesEachSupportedMessageType()
    {
        Assert.IsType<SnapshotRequest>(RelayMessageParser.Parse(MakeJsonPacket(JsonIdentifier.snap_req, "{}")));
        Assert.IsType<SnapshotDone>(RelayMessageParser.Parse(MakeJsonPacket(JsonIdentifier.snap_done, "{}")));
        Assert.IsType<SnapshotBegin>(RelayMessageParser.Parse(MakeJsonPacket(JsonIdentifier.snap_begin,
            "{\"Name\":\"world_snapshot.json\",\"Chunks\":4,\"Bytes\":3600}")));
        Assert.IsType<SnapshotEnd>(RelayMessageParser.Parse(MakeJsonPacket(JsonIdentifier.snap_end,
            "{\"Name\":\"world_snapshot.json\"}")));
        Assert.IsType<PlayerState>(RelayMessageParser.Parse(MakeJsonPacket(JsonIdentifier.player_id,
            "{\"player_id\":\"p1\"}")));
        Assert.IsType<RelayStateUpdate>(RelayMessageParser.Parse(MakeJsonPacket(JsonIdentifier.players, "{\"players\":[]}")));
    }

    [Fact]
    public void MessageParserRejectsMalformedJson()
    {
        var malformed = new RelayPacket(new RelayPacketKind.Json(JsonIdentifier.player_id), Encoding.UTF8.GetBytes("not json"));

        Assert.Null(RelayMessageParser.Parse(malformed));
    }

    [Fact]
    public void MessageParserRejectsNonJsonPacketKind()
    {
        var snapshotChunk = new RelayPacket(new RelayPacketKind.SnapshotChunk(SnapshotFileId.World, 0), []);

        Assert.Null(RelayMessageParser.Parse(snapshotChunk));
    }

    [Fact]
    public void PlayerStateDeserializesPlayerId()
    {
        var state = JsonSerializer.Deserialize<PlayerState>("{\"player_id\":\"p1\"}");

        Assert.NotNull(state);
        Assert.Equal("p1", state.PlayerId);
    }

    static RelayPacket MakeJsonPacket(JsonIdentifier identifier, string json) =>
        new(new RelayPacketKind.Json(identifier), Encoding.UTF8.GetBytes(json));
}
