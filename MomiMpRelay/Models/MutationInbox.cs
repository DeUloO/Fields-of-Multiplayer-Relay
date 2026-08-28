using System.Text.Json;
using System.Text.Json.Serialization;
using MomiMpRelay.Configuration;
using MomiMpRelay.Ledger;

namespace MomiMpRelay.Models;

/// <summary>A canonical, ordered slice of the ledger for one client, starting just after its applied cursor.</summary>
public sealed record MutationInboxBatch(
    [property: JsonPropertyName("protocol")] int Protocol,
    [property: JsonPropertyName("sessionId")] string SessionId,
    [property: JsonPropertyName("fromRelaySeq")] long FromRelaySeq,
    [property: JsonPropertyName("toRelaySeq")] long ToRelaySeq,
    [property: JsonPropertyName("events")] IReadOnlyList<MutationEnvelope> Events);

/// <summary>Builds canonical inbox batches from the ledger; does not touch files or the network.</summary>
public static class MutationInboxMaterializer
{
    public static MutationInboxBatch? BuildBatch(MutationLedger ledger, string sessionId, string playerId, int maxEvents)
    {
        ArgumentNullException.ThrowIfNull(ledger);

        var cursor = ledger.GetClientCursor(sessionId, playerId);
        var events = ledger.GetEventsAfter(sessionId, cursor, maxEvents);
        if (events.Count == 0)
            return null;

        return new MutationInboxBatch(RelaySession.ProtocolVersion, sessionId, events[0].RelaySeq,
            events[^1].RelaySeq, events);
    }
}

/// <summary>Publishes the pending inbox batch as one fixed-name, atomically-replaced file.</summary>
/// <remarks>
/// GML mods cannot list directory contents (mmapi does not expose file_find_first/next), so
/// clients cannot discover ranged batch files. A single known filename is used instead; the relay
/// overwrites it whenever the client's cursor has advanced past what was last published.
/// </remarks>
public static class MutationInboxPublisher
{
    public const string PendingBatchFileName = "pending-batch.json";

    public static string PublishAtomic(string inboxDir, MutationInboxBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        Directory.CreateDirectory(inboxDir);

        var final = Path.Combine(inboxDir, PendingBatchFileName);
        var tmp = final + ".tmp";
        File.WriteAllText(tmp, JsonSerializer.Serialize(batch, MutationJson.Options));
        File.Move(tmp, final, overwrite: true);
        return final;
    }
}
