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
    public async Task TryDeleteRemovesStatusFile()
    {
        var reporter = new StatusReporter(_directory);
        await reporter.WriteOnceAsync(CancellationToken.None);

        reporter.TryDelete();

        Assert.False(File.Exists(Path.Combine(_directory, "mp_status.json")));
    }

    public void Dispose()
    {
        try { Directory.Delete(_directory, recursive: true); } catch { }
    }
}
