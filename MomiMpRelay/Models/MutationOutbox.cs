using System.Text.Json;
using System.Text.Json.Serialization;
using MomiMpRelay.Ledger;

namespace MomiMpRelay.Models;

/// <summary>Acknowledges the durable state of one player's outbox, matching the durable outbox protocol.</summary>
public sealed record MutationOutboxAck(
    [property: JsonPropertyName("protocol")] int Protocol,
    [property: JsonPropertyName("playerId")] string PlayerId,
    [property: JsonPropertyName("clientEpoch")] string ClientEpoch,
    [property: JsonPropertyName("acceptedThroughClientSeq")] long AcceptedThroughClientSeq,
    [property: JsonPropertyName("relayHeadSeq")] long RelayHeadSeq);

public sealed record MutationOutboxIngestResult(int Accepted, int Duplicates, int MalformedLines, MutationOutboxAck? Ack);

/// <summary>Result of feeding a batch of envelopes (from either the local outbox or a network upload) into the ledger.</summary>
public readonly record struct EnvelopeAcceptSummary(int Accepted, int Duplicates, int Malformed, MutationEnvelope? Last, long MaxClientSeq);

/// <summary>Reads immutable outbox segment files, feeds accepted events into the durable ledger, and publishes an ack.</summary>
public static class MutationOutboxIngestor
{
    public const string AckFileName = "producer-ack.json";

    /// <summary>Accepts every envelope in one ledger transaction; a malformed/invalid entry is skipped, not fatal to the batch.</summary>
    public static EnvelopeAcceptSummary AcceptAll(MutationLedger ledger, IReadOnlyList<MutationEnvelope> envelopes)
    {
        ArgumentNullException.ThrowIfNull(ledger);
        ArgumentNullException.ThrowIfNull(envelopes);

        var results = ledger.AcceptMany(envelopes);

        int accepted = 0, duplicates = 0, malformed = 0;
        MutationEnvelope? last = null;
        long maxClientSeq = 0;

        for (var i = 0; i < envelopes.Count; i++)
        {
            var result = results[i];
            if (result is null)
            {
                malformed++;
                continue;
            }

            if (result.IsDuplicate) duplicates++; else accepted++;
            last = envelopes[i];
            if (envelopes[i].ClientSeq > maxClientSeq) maxClientSeq = envelopes[i].ClientSeq;
        }

        return new EnvelopeAcceptSummary(accepted, duplicates, malformed, last, maxClientSeq);
    }

    public static MutationOutboxIngestResult Ingest(string outboxDir, MutationLedger ledger)
    {
        ArgumentNullException.ThrowIfNull(ledger);
        if (!Directory.Exists(outboxDir))
            return new MutationOutboxIngestResult(0, 0, 0, null);

        var segments = Directory.GetFiles(outboxDir, "segment-*.json")
            .OrderBy(path => path, StringComparer.Ordinal);

        var parsed = new List<MutationEnvelope>();
        int malformed = 0;

        foreach (var segment in segments)
        {
            JsonElement[] rawEntries;
            try
            {
                rawEntries = JsonSerializer.Deserialize<JsonElement[]>(File.ReadAllBytes(segment)) ?? [];
            }
            catch (IOException)
            {
                continue; // still being written; retried on the next ingestion pass
            }
            catch (JsonException)
            {
                malformed++;
                continue;
            }

            foreach (var rawEntry in rawEntries)
            {
                try
                {
                    parsed.Add(rawEntry.Deserialize<MutationEnvelope>()
                        ?? throw new JsonException("Entry deserialized to null."));
                }
                catch (JsonException)
                {
                    malformed++;
                }
            }
        }

        var summary = AcceptAll(ledger, parsed);
        malformed += summary.Malformed;

        if (summary.Last is null)
            return new MutationOutboxIngestResult(summary.Accepted, summary.Duplicates, malformed, null);

        var ack = new MutationOutboxAck(
            summary.Last.Protocol, summary.Last.PlayerId, summary.Last.ClientEpoch, summary.MaxClientSeq, ledger.GetHeadRelaySeq(summary.Last.SessionId));
        WriteAckAtomic(outboxDir, ack);

        return new MutationOutboxIngestResult(summary.Accepted, summary.Duplicates, malformed, ack);
    }

    /// <summary>Returns entries this player's outbox has not yet had acknowledged, per the persisted producer-ack.json.</summary>
    public static MutationEnvelope[] GetUnacknowledged(string outboxDir)
    {
        var acceptedThroughClientSeq = ReadAcceptedThroughClientSeq(outboxDir);

        var segments = Directory.GetFiles(outboxDir, "segment-*.json")
            .OrderBy(path => path, StringComparer.Ordinal);

        var unacknowledged = new List<MutationEnvelope>();
        foreach (var segment in segments)
        {
            MutationEnvelope[] entries;
            try
            {
                entries = JsonSerializer.Deserialize<MutationEnvelope[]>(File.ReadAllBytes(segment)) ?? [];
            }
            catch (IOException)
            {
                continue; // still being written; retried on the next ingestion pass
            }
            catch (JsonException)
            {
                continue;
            }

            foreach (var entry in entries)
            {
                if (entry.ClientSeq > acceptedThroughClientSeq)
                    unacknowledged.Add(entry);
            }
        }

        return unacknowledged.ToArray();
    }

    static long ReadAcceptedThroughClientSeq(string outboxDir)
    {
        try
        {
            var path = Path.Combine(outboxDir, AckFileName);
            if (!File.Exists(path))
                return 0;
            var ack = JsonSerializer.Deserialize<MutationOutboxAck>(File.ReadAllText(path), MutationJson.Options);
            return ack?.AcceptedThroughClientSeq ?? 0;
        }
        catch (JsonException)
        {
            return 0;
        }
    }

    public static void WriteAckAtomic(string outboxDir, MutationOutboxAck ack)
    {
        var final = Path.Combine(outboxDir, AckFileName);
        var tmp = final + ".tmp";
        File.WriteAllText(tmp, JsonSerializer.Serialize(ack, MutationJson.Options));
        File.Move(tmp, final, overwrite: true);
    }
}
