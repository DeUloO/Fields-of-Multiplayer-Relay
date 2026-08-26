using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace MomiMpRelay.Models;

public static class RelayProtocol
{
    public const string ConnectionKey = "momi-mp";
}

public enum RelayPacketKind : byte
{
    Json = 1,
    SnapshotChunk = 2,
}

public readonly record struct RelayPacket(RelayPacketKind Kind, byte[] Data);

public enum SnapshotFileId : byte
{
    World = 1,
    Terrain = 2,
}

public readonly record struct SnapshotChunk(SnapshotFileId FileId, int Sequence, byte[] Data);

[JsonConverter(typeof(RelayControlJsonConverter))]
public sealed record RelayControl
{
    [JsonPropertyName("mode")]
    public string Mode { get; init; } = "off";

    [JsonPropertyName("ip")]
    public string Ip { get; init; } = "127.0.0.1";

    [JsonPropertyName("port")]
    public int Port
    {
        get; init;
    }

    [JsonPropertyName("seq")]
    public long Seq
    {
        get; init;
    }
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
        if (!root.TryGetProperty(propertyName, out var value))
            return 0;
        try
        {
            return value.GetInt32();
        }
        catch (FormatException) { return (int)value.GetDouble(); }
        catch (InvalidOperationException) { return 0; }
    }

    static long ReadLong(JsonElement root, string propertyName)
    {
        if (!root.TryGetProperty(propertyName, out var value))
            return 0;
        try
        {
            return value.GetInt64();
        }
        catch (FormatException) { return (long)value.GetDouble(); }
        catch (InvalidOperationException) { return 0; }
    }
}

public interface IRelayMessage
{
    string Identifier
    {
        get;
    }
    JsonObject ToJson();
}

public interface IMpControlMessage : IRelayMessage
{
    string MpMessage
    {
        get;
    }
}

public abstract record MpControlMessage : IMpControlMessage
{
    protected MpControlMessage(string mpMessage) => MpMessage = mpMessage;

    [JsonPropertyName("mp_msg")]
    public string MpMessage
    {
        get;
    }

    [JsonIgnore]
    public string Identifier => MpMessage;

    public JsonObject ToJson() =>
        JsonSerializer.SerializeToNode(this, GetType())!.AsObject();
}

public sealed record SnapshotRequest : MpControlMessage
{
    public SnapshotRequest() : base("snap_req") { }
}

public sealed record SnapshotDone : MpControlMessage
{
    public SnapshotDone() : base("snap_done") { }
}

public sealed record SnapshotBegin(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("chunks")] int Chunks,
    [property: JsonPropertyName("bytes")] int Bytes) : MpControlMessage("snap_begin");

public sealed record SnapshotEnd(
    [property: JsonPropertyName("name")] string Name) : MpControlMessage("snap_end");

public sealed record PlayerState(string PlayerId, JsonObject Payload) : IRelayMessage
{
    public string Identifier => "player_id";
    public JsonObject ToJson() => Payload;

    public static PlayerState? Parse(string json)
    {
        try
        {
            var payload = JsonNode.Parse(json)?.AsObject();
            return payload is null ? null : Parse(payload);
        }
        catch { return null; }
    }

    public static PlayerState? Parse(JsonObject payload)
    {
        var playerId = payload["player_id"]?.GetValue<string>();
        return string.IsNullOrWhiteSpace(playerId) ? null : new PlayerState(playerId, payload);
    }
}

public sealed record RelayStateUpdate(JsonObject Payload) : IRelayMessage
{
    public string Identifier => "players";
    public JsonObject ToJson() => Payload;
}

public static class RelayMessageParser
{
    public static IRelayMessage? Parse(string json)
    {
        try
        {
            var node = JsonNode.Parse(json)?.AsObject();
            return node is null ? null : Parse(node);
        }
        catch { return null; }
    }

    public static IRelayMessage? Parse(JsonObject node)
    {
        if (node["mp_msg"] is not null)
            return ParseControl(node);
        if (node["player_id"] is not null)
            return PlayerState.Parse(node);
        if (node["players"] is not null)
            return new RelayStateUpdate(node);
        return null;
    }

    public static IMpControlMessage? ParseControl(string json)
    {
        try
        {
            var node = JsonNode.Parse(json)?.AsObject();
            return node is null ? null : ParseControl(node);
        }
        catch { return null; }
    }

    static IMpControlMessage? ParseControl(JsonObject node) => node["mp_msg"]?.GetValue<string>() switch
    {
        "snap_req" => node.Deserialize<SnapshotRequest>(),
        "snap_done" => node.Deserialize<SnapshotDone>(),
        "snap_begin" => node.Deserialize<SnapshotBegin>(),
        "snap_end" => node.Deserialize<SnapshotEnd>(),
        _ => null,
    };
}
