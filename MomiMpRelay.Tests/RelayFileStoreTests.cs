using MomiMpRelay.FileSystem;

namespace MomiMpRelay.Tests;

public sealed class RelayFileStoreTests : IDisposable
{
    readonly string _directory = Path.Combine(Path.GetTempPath(), "MomiMpRelay.Tests", Guid.NewGuid().ToString("N"));

    public RelayFileStoreTests() => Directory.CreateDirectory(_directory);

    [Fact]
    public async Task ReadTextSharedReadsExistingFile()
    {
        var path = Path.Combine(_directory, "input.json");
        await File.WriteAllTextAsync(path, "{\"ok\":true}");

        var result = await RelayFileStore.ReadTextSharedAsync(path, CancellationToken.None);

        Assert.Equal("{\"ok\":true}", result);
    }

    [Fact]
    public async Task MissingFilesReturnNull()
    {
        var text = await RelayFileStore.ReadTextSharedAsync(Path.Combine(_directory, "missing"), CancellationToken.None);
        var bytes = await RelayFileStore.ReadBytesSharedAsync(Path.Combine(_directory, "missing"), CancellationToken.None);

        Assert.Null(text);
        Assert.Null(bytes);
    }

    [Fact]
    public async Task WriteRemoteReplacesExistingFile()
    {
        var path = Path.Combine(_directory, "remote.json");
        await File.WriteAllTextAsync(path, "old");
        using var writeLock = new SemaphoreSlim(1, 1);

        await RelayFileStore.WriteRemoteAsync(path, "new", writeLock, CancellationToken.None);

        Assert.Equal("new", await File.ReadAllTextAsync(path));
        Assert.False(File.Exists(path + ".tmp"));
    }

    public void Dispose()
    {
        try { Directory.Delete(_directory, recursive: true); } catch { }
    }
}
