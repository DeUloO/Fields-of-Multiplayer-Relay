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

/// <summary>Reads immutable outbox segment files, feeds accepted events into the durable ledger, and publishes an ack.</summary>
public static class MutationOutboxIngestor
{
    public const string AckFileName = "producer-ack.json";

    public static MutationOutboxIngestResult Ingest(string outboxDir, MutationLedger ledger)
    {
        ArgumentNullException.ThrowIfNull(ledger);
        if (!Directory.Exists(outboxDir))
            return new MutationOutboxIngestResult(0, 0, 0, null);

        var segments = Directory.GetFiles(outboxDir, "segment-*.jsonl")
            .OrderBy(path => path, StringComparer.Ordinal);

        int accepted = 0, duplicates = 0, malformed = 0;
        MutationEnvelope? last = null;
        long maxClientSeq = 0;

        foreach (var segment in segments)
        {
            string[] lines;
            try
            {
                lines = File.ReadAllLines(segment);
            }
            catch (IOException)
            {
                continue; // still being written; retried on the next ingestion pass
            }

            foreach (var line in lines)
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                MutationEnvelope envelope;
                try
                {
                    envelope = MutationJson.DeserializeAndValidate(line);
                }
                catch (JsonException)
                {
                    malformed++;
                    continue;
                }

                var result = ledger.Accept(envelope);
                if (result.IsDuplicate) duplicates++; else accepted++;

                last = envelope;
                if (envelope.ClientSeq > maxClientSeq) maxClientSeq = envelope.ClientSeq;
            }
        }

        if (last is null)
            return new MutationOutboxIngestResult(accepted, duplicates, malformed, null);

        var ack = new MutationOutboxAck(
            last.Protocol, last.PlayerId, last.ClientEpoch, maxClientSeq, ledger.GetHeadRelaySeq(last.SessionId));
        WriteAckAtomic(outboxDir, ack);

        return new MutationOutboxIngestResult(accepted, duplicates, malformed, ack);
    }

    static void WriteAckAtomic(string outboxDir, MutationOutboxAck ack)
    {
        var final = Path.Combine(outboxDir, AckFileName);
        var tmp = final + ".tmp";
        File.WriteAllText(tmp, JsonSerializer.Serialize(ack, MutationJson.Options));
        File.Move(tmp, final, overwrite: true);
    }
}
