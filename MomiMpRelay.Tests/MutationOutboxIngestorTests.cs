using System.Text.Json;
using MomiMpRelay.Ledger;
using MomiMpRelay.Models;

namespace MomiMpRelay.Tests;

public sealed class MutationOutboxIngestorTests : IDisposable
{
    readonly string _directory = Path.Combine(Path.GetTempPath(), "MomiMpRelay.Tests", Guid.NewGuid().ToString("N"));
    readonly string _outboxDir;
    readonly MutationLedger _ledger;

    public MutationOutboxIngestorTests()
    {
        Directory.CreateDirectory(_directory);
        _outboxDir = Path.Combine(_directory, "outbox");
        Directory.CreateDirectory(_outboxDir);
        _ledger = new MutationLedger(_directory);
    }

    [Fact]
    public void IngestAcceptsEnvelopesAcrossOrderedSegmentsAndWritesAck()
    {
        WriteSegment("segment-000001.json",
            EnvelopeLine(clientSeq: 1, eventId: "p1:e1:1"),
            EnvelopeLine(clientSeq: 2, eventId: "p1:e1:2"));
        WriteSegment("segment-000002.json",
            EnvelopeLine(clientSeq: 3, eventId: "p1:e1:3"));

        var result = MutationOutboxIngestor.Ingest(_outboxDir, _ledger);

        Assert.Equal(3, result.Accepted);
        Assert.Equal(0, result.Duplicates);
        Assert.Equal(0, result.MalformedLines);
        Assert.NotNull(result.Ack);
        Assert.Equal(3, result.Ack!.AcceptedThroughClientSeq);
        Assert.Equal(3, result.Ack.RelayHeadSeq);
        Assert.Equal("Farmer|Farm", result.Ack.PlayerId);

        var ackPath = Path.Combine(_outboxDir, MutationOutboxIngestor.AckFileName);
        Assert.True(File.Exists(ackPath));
        var persisted = JsonSerializer.Deserialize<MutationOutboxAck>(File.ReadAllText(ackPath), MutationJson.Options);
        Assert.Equal(3, persisted!.AcceptedThroughClientSeq);
    }

    [Fact]
    public void ReingestingTheSameSegmentsIsIdempotent()
    {
        WriteSegment("segment-000001.json", EnvelopeLine(clientSeq: 1, eventId: "p1:e1:1"));

        var first = MutationOutboxIngestor.Ingest(_outboxDir, _ledger);
        var second = MutationOutboxIngestor.Ingest(_outboxDir, _ledger);

        Assert.Equal(1, first.Accepted);
        Assert.Equal(0, second.Accepted);
        Assert.Equal(1, second.Duplicates);
        Assert.Equal(1, second.Ack!.RelayHeadSeq);
    }

    [Fact]
    public void MalformedEntriesAreSkippedWithoutStoppingIngestion()
    {
        WriteSegment("segment-000001.json",
            EnvelopeLine(clientSeq: 1, eventId: "p1:e1:1"),
            "\"not an envelope\"",
            EnvelopeLine(clientSeq: 2, eventId: "p1:e1:2"));

        var result = MutationOutboxIngestor.Ingest(_outboxDir, _ledger);

        Assert.Equal(2, result.Accepted);
        Assert.Equal(1, result.MalformedLines);
        Assert.Equal(2, result.Ack!.AcceptedThroughClientSeq);
    }

    [Fact]
    public void MissingOutboxDirectoryProducesAnEmptyResult()
    {
        var result = MutationOutboxIngestor.Ingest(Path.Combine(_directory, "does-not-exist"), _ledger);

        Assert.Equal(0, result.Accepted);
        Assert.Null(result.Ack);
    }

    [Fact]
    public void GetUnacknowledgedComparesAcceptedThroughClientSeqNotRelayHeadSeq()
    {
        // Ingest and ack this player's clientSeq 1, then inflate the session's global relaySeq
        // far past clientSeq 2 via a second producer, to prove unacked filtering doesn't use RelayHeadSeq.
        WriteSegment("segment-000001.json", EnvelopeLine(clientSeq: 1, eventId: "p1:e1:1"));
        MutationOutboxIngestor.Ingest(_outboxDir, _ledger);
        for (var i = 0; i < 5; i++)
            _ledger.Accept(new MutationEnvelope(2, "session-1", "Other|Farm", "epoch-2", i + 1, $"other:e2:{i + 1}",
                new ItemPickupMutation(Sequence: i + 1, LocationId: 1, ItemGid: "9:9:9:9")));
        Assert.True(_ledger.GetHeadRelaySeq("session-1") > 2);

        WriteSegment("segment-000002.json", EnvelopeLine(clientSeq: 2, eventId: "p1:e1:2"));

        var unacknowledged = MutationOutboxIngestor.GetUnacknowledged(_outboxDir);

        Assert.Single(unacknowledged);
        Assert.Equal(2, unacknowledged[0].ClientSeq);
    }

    void WriteSegment(string name, params string[] rawJsonEntries) =>
        File.WriteAllText(Path.Combine(_outboxDir, name), $"[{string.Join(",", rawJsonEntries)}]");

    static string EnvelopeLine(long clientSeq, string eventId) => JsonSerializer.Serialize(
        new MutationEnvelope(2, "session-1", "Farmer|Farm", "epoch-1", clientSeq, eventId,
            new ItemPickupMutation(Sequence: (int)clientSeq, LocationId: 1, ItemGid: "1:1:1:1")),
        MutationJson.Options);

    public void Dispose()
    {
        _ledger.Dispose();
        try
        {
            Directory.Delete(_directory, recursive: true);
        }
        catch { }
    }
}
