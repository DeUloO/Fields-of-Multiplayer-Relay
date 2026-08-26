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
    public void ControlParserCreatesTypedSnapshotBegin()
    {
        var message = RelayMessageParser.ParseControl(
            "{\"mp_msg\":\"snap_begin\",\"name\":\"world_snapshot.json\",\"chunks\":4,\"bytes\":3600}");

        var begin = Assert.IsType<SnapshotBegin>(message);
        Assert.Equal("snap_begin", begin.MpMessage);
        Assert.Equal("world_snapshot.json", begin.Name);
        Assert.Equal(4, begin.Chunks);
        Assert.Equal(3600, begin.Bytes);
    }

    [Fact]
    public void SnapshotBeginSerializationIncludesDerivedMetadata()
    {
        var json = new SnapshotBegin("world_snapshot.json", 4, 3600).ToJson();

        Assert.Equal("snap_begin", json["mp_msg"]!.GetValue<string>());
        Assert.Equal("world_snapshot.json", json["name"]!.GetValue<string>());
        Assert.Equal(4, json["chunks"]!.GetValue<int>());
        Assert.Equal(3600, json["bytes"]!.GetValue<int>());
    }

    [Theory]
    [InlineData("{\"mp_msg\":\"unknown\"}")]
    [InlineData("not json")]
    public void ControlParserRejectsUnknownOrMalformedMessages(string json)
    {
        Assert.Null(RelayMessageParser.ParseControl(json));
    }

    [Fact]
    public void MessageParserDispatchesEachSupportedMessageType()
    {
        Assert.IsType<SnapshotRequest>(RelayMessageParser.Parse("{\"mp_msg\":\"snap_req\"}"));
        Assert.IsType<PlayerState>(RelayMessageParser.Parse("{\"player_id\":\"p1\"}"));
        Assert.IsType<RelayStateUpdate>(RelayMessageParser.Parse("{\"players\":[]}"));
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("{\"mp_msg\":\"unknown\"}")]
    [InlineData("not json")]
    public void MessageParserRejectsUnknownMessages(string json)
    {
        Assert.Null(RelayMessageParser.Parse(json));
    }

    [Fact]
    public void PlayerStateRequiresNonBlankPlayerId()
    {
        var state = PlayerState.Parse("{\"player_id\":\"p1\",\"x\":12}");
        var missing = PlayerState.Parse("{\"x\":12}");
        var blank = PlayerState.Parse("{\"player_id\":\" \"}");

        Assert.NotNull(state);
        Assert.Equal("p1", state.PlayerId);
        Assert.Equal(12, state.Payload["x"]!.GetValue<int>());
        Assert.Null(missing);
        Assert.Null(blank);
    }
}
