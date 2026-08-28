using MomiMpRelay.Ledger;
using MomiMpRelay.Models;

namespace MomiMpRelay.Tests;

public sealed class MutationLedgerTests : IDisposable
{
    readonly string _directory = Path.Combine(Path.GetTempPath(), "MomiMpRelay.Tests", Guid.NewGuid().ToString("N"));

    public MutationLedgerTests() => Directory.CreateDirectory(_directory);

    [Fact]
    public void AcceptAllocatesSequentialRelaySeqPerSession()
    {
        using var ledger = new MutationLedger(_directory);

        var first = ledger.Accept(MakeEnvelope(clientSeq: 1, eventId: "p1:e1:1"));
        var second = ledger.Accept(MakeEnvelope(clientSeq: 2, eventId: "p1:e1:2"));

        Assert.Equal(1, first.RelaySeq);
        Assert.False(first.IsDuplicate);
        Assert.Equal(2, second.RelaySeq);
        Assert.False(second.IsDuplicate);
        Assert.Equal(2, ledger.GetHeadRelaySeq("session-1"));
    }

    [Fact]
    public void AcceptIsIdempotentForARepeatedEventId()
    {
        using var ledger = new MutationLedger(_directory);

        var first = ledger.Accept(MakeEnvelope(clientSeq: 1, eventId: "p1:e1:1"));
        var retry = ledger.Accept(MakeEnvelope(clientSeq: 1, eventId: "p1:e1:1"));

        Assert.Equal(first.RelaySeq, retry.RelaySeq);
        Assert.False(first.IsDuplicate);
        Assert.True(retry.IsDuplicate);
        Assert.Equal(1, ledger.GetHeadRelaySeq("session-1"));
    }

    [Fact]
    public void AcceptAllocatesIndependentSequencesPerSession()
    {
        using var ledger = new MutationLedger(_directory);

        var a = ledger.Accept(MakeEnvelope(sessionId: "session-a", clientSeq: 1, eventId: "p1:e1:1"));
        var b = ledger.Accept(MakeEnvelope(sessionId: "session-b", clientSeq: 1, eventId: "p1:e1:1"));

        Assert.Equal(1, a.RelaySeq);
        Assert.Equal(1, b.RelaySeq);
    }

    [Fact]
    public void LedgerPersistsAcrossInstancesAgainstTheSameFile()
    {
        using (var ledger = new MutationLedger(_directory))
            ledger.Accept(MakeEnvelope(clientSeq: 1, eventId: "p1:e1:1"));

        using var reopened = new MutationLedger(_directory);

        Assert.Equal(1, reopened.GetHeadRelaySeq("session-1"));
        var retry = reopened.Accept(MakeEnvelope(clientSeq: 1, eventId: "p1:e1:1"));
        Assert.True(retry.IsDuplicate);
        Assert.Equal(1, retry.RelaySeq);
    }

    [Fact]
    public void AcceptRejectsAnInvalidEnvelope()
    {
        using var ledger = new MutationLedger(_directory);
        var invalid = MakeEnvelope(clientSeq: 1, eventId: "p1:e1:1") with { SessionId = " " };

        Assert.ThrowsAny<Exception>(() => ledger.Accept(invalid));
    }

    static MutationEnvelope MakeEnvelope(long clientSeq, string eventId, string sessionId = "session-1") =>
        new(
            Protocol: 2,
            SessionId: sessionId,
            PlayerId: "Farmer|Farm",
            ClientEpoch: "epoch-1",
            ClientSeq: clientSeq,
            EventId: eventId,
            RelaySeq: 0,
            Event: new ItemPickupMutation(Sequence: (int)clientSeq, LocationId: 1, ItemGid: "1:1:1:1"));

    public void Dispose()
    {
        try
        {
            Directory.Delete(_directory, recursive: true);
        }
        catch { }
    }
}
