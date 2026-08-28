using System.Text.Json;
using MomiMpRelay.Ledger;
using MomiMpRelay.Models;

namespace MomiMpRelay.Tests;

public sealed class MutationInboxTests : IDisposable
{
    readonly string _directory = Path.Combine(Path.GetTempPath(), "MomiMpRelay.Tests", Guid.NewGuid().ToString("N"));
    readonly MutationLedger _ledger;

    public MutationInboxTests()
    {
        Directory.CreateDirectory(_directory);
        _ledger = new MutationLedger(_directory);
    }

    [Fact]
    public void BuildBatchReturnsNullWhenClientIsCaughtUp()
    {
        var batch = MutationInboxMaterializer.BuildBatch(_ledger, "session-1", "Farmer|Farm", maxEvents: 10);

        Assert.Null(batch);
    }

    [Fact]
    public void BuildBatchReturnsEventsAfterTheClientCursor()
    {
        Accept(clientSeq: 1, eventId: "p1:e1:1");
        Accept(clientSeq: 2, eventId: "p1:e1:2");
        Accept(clientSeq: 3, eventId: "p1:e1:3");
        _ledger.RecordClientCursor("session-1", "Farmer|Farm", relaySeq: 1);

        var batch = MutationInboxMaterializer.BuildBatch(_ledger, "session-1", "Farmer|Farm", maxEvents: 10);

        Assert.NotNull(batch);
        Assert.Equal(2, batch!.FromRelaySeq);
        Assert.Equal(3, batch.ToRelaySeq);
        Assert.Equal(2, batch.Events.Count);
        Assert.Equal(2, batch.Events[0].RelaySeq);
        Assert.Equal(3, batch.Events[1].RelaySeq);
        Assert.IsType<ItemPickupMutation>(batch.Events[0].Event);
    }

    [Fact]
    public void BuildBatchRespectsMaxEventsCap()
    {
        Accept(clientSeq: 1, eventId: "p1:e1:1");
        Accept(clientSeq: 2, eventId: "p1:e1:2");
        Accept(clientSeq: 3, eventId: "p1:e1:3");

        var batch = MutationInboxMaterializer.BuildBatch(_ledger, "session-1", "Farmer|Farm", maxEvents: 2);

        Assert.NotNull(batch);
        Assert.Equal(2, batch!.Events.Count);
        Assert.Equal(1, batch.FromRelaySeq);
        Assert.Equal(2, batch.ToRelaySeq);
    }

    [Fact]
    public void RecordClientCursorNeverMovesBackward()
    {
        _ledger.RecordClientCursor("session-1", "Farmer|Farm", relaySeq: 5);
        _ledger.RecordClientCursor("session-1", "Farmer|Farm", relaySeq: 2);

        Assert.Equal(5, _ledger.GetClientCursor("session-1", "Farmer|Farm"));
    }

    [Fact]
    public void PublishAtomicWritesTheFixedPendingBatchFile()
    {
        Accept(clientSeq: 1, eventId: "p1:e1:1");
        var batch = MutationInboxMaterializer.BuildBatch(_ledger, "session-1", "Farmer|Farm", maxEvents: 10)!;
        var inboxDir = Path.Combine(_directory, "inbox");

        var path = MutationInboxPublisher.PublishAtomic(inboxDir, batch);

        Assert.Equal(MutationInboxPublisher.PendingBatchFileName, Path.GetFileName(path));
        Assert.False(File.Exists(path + ".tmp"));
        var persisted = JsonSerializer.Deserialize<MutationInboxBatch>(File.ReadAllText(path), MutationJson.Options);
        Assert.Equal(1, persisted!.FromRelaySeq);
        Assert.Single(persisted.Events);
    }

    void Accept(long clientSeq, string eventId) => _ledger.Accept(
        new MutationEnvelope(2, "session-1", "Farmer|Farm", "epoch-1", clientSeq, eventId,
            new ItemPickupMutation(Sequence: (int)clientSeq, LocationId: 1, ItemGid: "1:1:1:1")));

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
