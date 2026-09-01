using System.Text.Json.Nodes;
using MomiMpRelay.Status;

namespace MomiMpRelay.Tests;

public sealed class StatusReporterTests : IDisposable
{
    readonly string _directory = Path.Combine(Path.GetTempPath(), "MomiMpRelay.Tests", Guid.NewGuid().ToString("N"));

    public StatusReporterTests() => Directory.CreateDirectory(_directory);

    [Fact]
    public async Task WriteOncePublishesCurrentStatus()
    {
        var reporter = new StatusReporter(_directory);
        reporter.Set("connected", "join", 2, "ready");

        await reporter.WriteOnceAsync(CancellationToken.None);

        var path = Path.Combine(_directory, "mp_status.json");
        var status = JsonNode.Parse(await File.ReadAllTextAsync(path))!.AsObject();
        Assert.Equal(1, status["hb"]!.GetValue<long>());
        Assert.Equal("connected", status["state"]!.GetValue<string>());
        Assert.Equal("join", status["role"]!.GetValue<string>());
        Assert.Equal(2, status["peers"]!.GetValue<int>());
        Assert.Equal("ready", status["detail"]!.GetValue<string>());
    }

    [Fact]
    public async Task WriteOncePublishesMutationDiagnostics()
    {
        var reporter = new StatusReporter(_directory);
        reporter.SetMutationDiagnostics(42, new Dictionary<string, long> { ["Farmer|Farm"] = 3 }, outboxPending: 2, inboxPending: 1);

        await reporter.WriteOnceAsync(CancellationToken.None);

        var path = Path.Combine(_directory, "mp_status.json");
        var status = JsonNode.Parse(await File.ReadAllTextAsync(path))!.AsObject();
        Assert.Equal(42, status["ledgerHeadRelaySeq"]!.GetValue<long>());
        Assert.Equal(3, status["clientLag"]!["Farmer|Farm"]!.GetValue<long>());
        Assert.Equal(2, status["outboxPending"]!.GetValue<int>());
        Assert.Equal(1, status["inboxPending"]!.GetValue<int>());
    }

    [Fact]
    public async Task TryDeleteRemovesStatusFile()
    {
        var reporter = new StatusReporter(_directory);
        await reporter.WriteOnceAsync(CancellationToken.None);

        reporter.TryDelete();

        Assert.False(File.Exists(Path.Combine(_directory, "mp_status.json")));
    }

    [Fact]
    public async Task RunStopsWhenCancelled()
    {
        var reporter = new StatusReporter(_directory);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await reporter.RunAsync(cts.Token);

        Assert.False(File.Exists(Path.Combine(_directory, "mp_status.json")));
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(_directory, recursive: true);
        }
        catch { }
    }
}
