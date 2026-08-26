using MomiMpRelay.Models;
using MomiMpRelay.Snapshots;

namespace MomiMpRelay.Tests;

public sealed class SnapshotReceiverTests : IDisposable
{
    readonly string _directory = Path.Combine(Path.GetTempPath(), "MomiMpRelay.Tests", Guid.NewGuid().ToString("N"));

    public SnapshotReceiverTests() => Directory.CreateDirectory(_directory);

    [Fact]
    public async Task CompleteSnapshotIsMovedIntoPlace()
    {
        var receiver = new SnapshotReceiver(_directory);
        var content = new byte[] { 10, 20, 30 };

        await receiver.HandleAsync(new SnapshotBegin("world_farm_terrain.bin", 2, content.Length), CancellationToken.None);
        await receiver.HandleChunkAsync(2, 0, content[..2], CancellationToken.None);
        await receiver.HandleChunkAsync(2, 1, content[2..], CancellationToken.None);
        await receiver.HandleAsync(new SnapshotEnd("world_farm_terrain.bin"), CancellationToken.None);

        Assert.Equal(content, await File.ReadAllBytesAsync(Path.Combine(_directory, "world_farm_terrain.bin")));
        Assert.False(File.Exists(Path.Combine(_directory, "world_farm_terrain.bin.part")));
    }

    [Fact]
    public async Task OutOfOrderChunkPreventsSnapshotCommit()
    {
        var receiver = new SnapshotReceiver(_directory);

        await receiver.HandleAsync(new SnapshotBegin("world_snapshot.json", 2, 2), CancellationToken.None);
        await receiver.HandleChunkAsync(1, 1, new byte[] { 2 }, CancellationToken.None);
        await receiver.HandleAsync(new SnapshotEnd("world_snapshot.json"), CancellationToken.None);

        Assert.False(File.Exists(Path.Combine(_directory, "world_snapshot.json")));
    }

    public void Dispose()
    {
        try { Directory.Delete(_directory, recursive: true); } catch { }
    }
}
