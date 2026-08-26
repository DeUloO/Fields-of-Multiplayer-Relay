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

    [Fact]
    public async Task ReadTextRetriesAfterTransientFileLock()
    {
        var path = Path.Combine(_directory, "locked.json");
        await File.WriteAllTextAsync(path, "ready");
        await using var locked = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.None);
        var read = RelayFileStore.ReadTextSharedAsync(path, CancellationToken.None);
        await Task.Delay(30);
        await locked.DisposeAsync();

        Assert.Equal("ready", await read);
    }

    [Fact]
    public async Task WriteRemoteReportsFailureWhenDirectoryDoesNotExist()
    {
        using var writeLock = new SemaphoreSlim(1, 1);
        var path = Path.Combine(_directory, "missing", "remote.json");

        var result = await RelayFileStore.WriteRemoteAsync(path, "data", writeLock, CancellationToken.None);

        Assert.False(result);
    }

    [Fact]
    public void CleanupTemporaryFilesRemovesStaleArtifacts()
    {
        var temp = Path.Combine(_directory, "status.tmp");
        var part = Path.Combine(_directory, "snapshot.part");
        File.WriteAllText(temp, "temporary");
        File.WriteAllText(part, "partial");

        RelayFileStore.CleanupTemporaryFiles(_directory);

        Assert.False(File.Exists(temp));
        Assert.False(File.Exists(part));
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
