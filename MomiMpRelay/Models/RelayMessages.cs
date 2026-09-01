using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace MomiMpRelay.Models;

public static class RelayProtocol
{
    public const string ConnectionKey = "momi-mp";
}

public enum RelayPacketKindType : byte
{
    Json = 1,
    SnapshotChunk = 2,
}
public record RelayPacketKind
{
    public RelayPacketKindType Type { get; init; }
    protected RelayPacketKind(RelayPacketKindType type) => Type = type;
    public record Json(JsonIdentifier Identifier) : RelayPacketKind(RelayPacketKindType.Json);
    public record SnapshotChunk(SnapshotFileId FileId, int Sequence) : RelayPacketKind(RelayPacketKindType.SnapshotChunk);
}

public readonly record struct RelayPacket(RelayPacketKind Kind, byte[] Data);

public enum SnapshotFileId : byte
{
    World = 1,
    Terrain = 2,
}

public enum JsonIdentifier : byte
{
    player_id,
    players,
    snap_req,
    snap_done,
    snap_begin,
    snap_end,
    mutation_batch_upload,
    mutation_batch_upload_ack,
    mutation_batch_download,
    repair_request,
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
        JsonSerializerOptions options) => throw new NotSupportedException(); // File is written by GML, only need to support read.

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

public interface IRelayPacket {};
public interface IMpControlMessage : IRelayPacket {};

public sealed record SnapshotRequest() : IMpControlMessage;

public sealed record SnapshotDone() : IMpControlMessage;

public sealed record SnapshotBegin(string Name, int Chunks, int Bytes) : IMpControlMessage;

public sealed record SnapshotEnd(string Name) : IMpControlMessage;

public sealed record PlayerState() : IRelayPacket
{
    [JsonPropertyName("player_id")] 
    public required string PlayerId { get; init; }
}

public sealed record RelayStateUpdate : IRelayPacket
{
    [JsonPropertyName("players")]
    public List<PlayerState> States { get; init; } = [];
    public RelayStateUpdate() {}
    public RelayStateUpdate(ConcurrentDictionary<string, PlayerState> states, string? exclude)
    {
        States = states.Values.Where(s => s.PlayerId != exclude).ToList();
    }
}

// Client -> Host: raw outbox entries this client hasn't had acknowledged yet.
public sealed record MutationBatchUpload(MutationEnvelope[] Entries) : IRelayPacket;

// Host -> Client: an ack for the client's own outbox (mirrors MutationOutboxAck).
public sealed record MutationBatchUploadAck(MutationOutboxAck Ack) : IRelayPacket;

// Host -> Client: a canonical inbox batch (mirrors MutationInboxBatch).
public sealed record MutationBatchDownload(MutationInboxBatch Batch) : IRelayPacket;

// Client -> Host: this client's inbox is stuck on a persistent gap; resync it from reportedCursor.
public sealed record RepairRequest(
    [property: JsonPropertyName("playerId")] string PlayerId,
    [property: JsonPropertyName("reportedCursor")] long ReportedCursor,
    [property: JsonPropertyName("reason")] string Reason) : IRelayPacket;

public static class RelayMessageParser
{
    public static IRelayPacket? Parse(RelayPacket packet)
    {
        try
        {
            switch ((packet.Kind as RelayPacketKind.Json)?.Identifier)
            {
                case JsonIdentifier.snap_req: return JsonSerializer.Deserialize<SnapshotRequest>(packet.Data);
                case JsonIdentifier.snap_done: return JsonSerializer.Deserialize<SnapshotDone>(packet.Data);
                case JsonIdentifier.snap_begin: return JsonSerializer.Deserialize<SnapshotBegin>(packet.Data);
                case JsonIdentifier.snap_end: return JsonSerializer.Deserialize<SnapshotEnd>(packet.Data);
                case JsonIdentifier.player_id: return JsonSerializer.Deserialize<PlayerState>(packet.Data);
                case JsonIdentifier.players: return JsonSerializer.Deserialize<RelayStateUpdate>(packet.Data);
                case JsonIdentifier.mutation_batch_upload: return JsonSerializer.Deserialize<MutationBatchUpload>(packet.Data);
                case JsonIdentifier.mutation_batch_upload_ack: return JsonSerializer.Deserialize<MutationBatchUploadAck>(packet.Data);
                case JsonIdentifier.mutation_batch_download: return JsonSerializer.Deserialize<MutationBatchDownload>(packet.Data);
                case JsonIdentifier.repair_request: return JsonSerializer.Deserialize<RepairRequest>(packet.Data);
                default: return null;
            }
        }
        catch { return null; }
    }
}
