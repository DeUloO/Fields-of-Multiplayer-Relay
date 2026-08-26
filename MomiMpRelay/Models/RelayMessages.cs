using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace MomiMpRelay.Models;

enum RelayPacketKind : byte
{
    Json = 1,
    SnapshotChunk = 2,
}

readonly record struct RelayPacket(RelayPacketKind Kind, byte[] Data);

[JsonConverter(typeof(RelayControlJsonConverter))]
sealed record RelayControl
{
    [JsonPropertyName("mode")]
    public string Mode { get; init; } = "off";

    [JsonPropertyName("ip")]
    public string Ip { get; init; } = "127.0.0.1";

    [JsonPropertyName("port")]
    public int Port { get; init; }

    [JsonPropertyName("seq")]
    public long Seq { get; init; }
}

sealed class RelayControlJsonConverter : JsonConverter<RelayControl>
{
    public override RelayControl Read(ref Utf8JsonReader reader, Type typeToConvert,
        JsonSerializerOptions options)
    {
        using var document = JsonDocument.ParseValue(ref reader);
        var root = document.RootElement;

        return new RelayControl
        {
            Mode = root.TryGetProperty("mode", out var mode)
                ? mode.GetString() ?? "off"
                : "off",
            Ip = root.TryGetProperty("ip", out var ip)
                ? ip.GetString() ?? "127.0.0.1"
                : "127.0.0.1",
            Port = ReadInt(root, "port"),
            Seq = ReadLong(root, "seq"),
        };
    }

    public override void Write(Utf8JsonWriter writer, RelayControl value,
        JsonSerializerOptions options) => throw new NotSupportedException();

    static int ReadInt(JsonElement root, string propertyName)
    {
        if (!root.TryGetProperty(propertyName, out var value)) return 0;
        try { return value.GetInt32(); }
        catch (FormatException) { return (int)value.GetDouble(); }
        catch (InvalidOperationException) { return 0; }
    }

    static long ReadLong(JsonElement root, string propertyName)
    {
        if (!root.TryGetProperty(propertyName, out var value)) return 0;
        try { return value.GetInt64(); }
        catch (FormatException) { return (long)value.GetDouble(); }
        catch (InvalidOperationException) { return 0; }
    }
}

interface IRelayMessage
{
    string Identifier { get; }
    JsonObject ToJson();
}

interface IMpControlMessage : IRelayMessage
{
    string MpMessage { get; }
}

abstract record MpControlMessage : IMpControlMessage
{
    protected MpControlMessage(string mpMessage) => MpMessage = mpMessage;

    [JsonPropertyName("mp_msg")]
    public string MpMessage { get; }

    [JsonIgnore]
    public string Identifier => MpMessage;

    public JsonObject ToJson() => JsonSerializer.SerializeToNode(this)!.AsObject();
}

sealed record SnapshotRequest : MpControlMessage
{
    public SnapshotRequest() : base("snap_req") { }
}

sealed record SnapshotDone : MpControlMessage
{
    public SnapshotDone() : base("snap_done") { }
}

sealed record SnapshotBegin(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("chunks")] int Chunks,
    [property: JsonPropertyName("bytes")] int Bytes) : MpControlMessage("snap_begin");

sealed record SnapshotEnd(
    [property: JsonPropertyName("name")] string Name) : MpControlMessage("snap_end");

sealed record PlayerState(string PlayerId, JsonObject Payload) : IRelayMessage
{
    public string Identifier => "player_id";
    public JsonObject ToJson() => Payload;

    public static PlayerState? Parse(string json)
    {
        try
        {
            var payload = JsonNode.Parse(json)?.AsObject();
            var playerId = payload?["player_id"]?.GetValue<string>();
            return payload is null || string.IsNullOrWhiteSpace(playerId)
                ? null
                : new PlayerState(playerId, payload);
        }
        catch { return null; }
    }
}

sealed record RelayStateUpdate(JsonObject Payload) : IRelayMessage
{
    public string Identifier => "players";
    public JsonObject ToJson() => Payload;
}

static class RelayMessageParser
{
    public static IMpControlMessage? ParseControl(string json)
    {
        try
        {
            var node = JsonNode.Parse(json)?.AsObject();
            return node?["mp_msg"]?.GetValue<string>() switch
            {
                "snap_req" => node.Deserialize<SnapshotRequest>(),
                "snap_done" => node.Deserialize<SnapshotDone>(),
                "snap_begin" => node.Deserialize<SnapshotBegin>(),
                "snap_end" => node.Deserialize<SnapshotEnd>(),
                _ => null,
            };
        }
        catch { return null; }
    }
}
