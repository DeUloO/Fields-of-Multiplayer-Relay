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
        await receiver.HandleChunkAsync(SnapshotFileId.Terrain, 0, content[..2], CancellationToken.None);
        await receiver.HandleChunkAsync(SnapshotFileId.Terrain, 1, content[2..], CancellationToken.None);
        await receiver.HandleAsync(new SnapshotEnd("world_farm_terrain.bin"), CancellationToken.None);

        Assert.Equal(content, await File.ReadAllBytesAsync(Path.Combine(_directory, "world_farm_terrain.bin")));
        Assert.False(File.Exists(Path.Combine(_directory, "world_farm_terrain.bin.part")));
    }

    [Fact]
    public async Task OutOfOrderChunkPreventsSnapshotCommit()
    {
        var receiver = new SnapshotReceiver(_directory);

        await receiver.HandleAsync(new SnapshotBegin("world_snapshot.json", 2, 2), CancellationToken.None);
        await receiver.HandleChunkAsync(SnapshotFileId.World, 1, new byte[] { 2 }, CancellationToken.None);
        await receiver.HandleAsync(new SnapshotEnd("world_snapshot.json"), CancellationToken.None);

        Assert.False(File.Exists(Path.Combine(_directory, "world_snapshot.json")));
    }

    [Fact]
    public async Task SnapshotDoneWaitsForEveryStartedFile()
    {
        var receiver = new SnapshotReceiver(_directory);
        var world = new byte[] { 1 };
        var terrain = new byte[] { 2, 3 };

        await receiver.HandleAsync(new SnapshotBegin("world_snapshot.json", 1, world.Length), CancellationToken.None);
        await receiver.HandleChunkAsync(SnapshotFileId.World, 0, world, CancellationToken.None);
        await receiver.HandleAsync(new SnapshotEnd("world_snapshot.json"), CancellationToken.None);
        await receiver.HandleAsync(new SnapshotBegin("world_farm_terrain.bin", 1, terrain.Length), CancellationToken.None);
        await receiver.HandleAsync(new SnapshotDone(), CancellationToken.None);

        Assert.False(File.Exists(Path.Combine(_directory, "mp_apply_world")));

        await receiver.HandleAsync(new SnapshotBegin("world_snapshot.json", 1, world.Length), CancellationToken.None);
        await receiver.HandleChunkAsync(SnapshotFileId.World, 0, world, CancellationToken.None);
        await receiver.HandleAsync(new SnapshotEnd("world_snapshot.json"), CancellationToken.None);
        await receiver.HandleAsync(new SnapshotBegin("world_farm_terrain.bin", 1, terrain.Length), CancellationToken.None);
        await receiver.HandleChunkAsync(SnapshotFileId.Terrain, 0, terrain, CancellationToken.None);
        await receiver.HandleAsync(new SnapshotEnd("world_farm_terrain.bin"), CancellationToken.None);
        await receiver.HandleAsync(new SnapshotDone(), CancellationToken.None);

        Assert.True(File.Exists(Path.Combine(_directory, "mp_apply_world")));
    }

    [Fact]
    public async Task CancellationDuringChunkWriteCanBeCleanedUp()
    {
        using var receiver = new SnapshotReceiver(_directory);
        using var cts = new CancellationTokenSource();
        await receiver.HandleAsync(new SnapshotBegin("world_snapshot.json", 1, 1), CancellationToken.None);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            receiver.HandleChunkAsync(SnapshotFileId.World, 0, new byte[] { 1 }, cts.Token));

        receiver.Dispose();
        Assert.False(File.Exists(Path.Combine(_directory, "world_snapshot.json.part")));
    }

    [Fact]
    public async Task InvalidSnapshotMetadataDoesNotCreatePartialFile()
    {
        using var receiver = new SnapshotReceiver(_directory);

        await receiver.HandleAsync(new SnapshotBegin("unknown.bin", 1, 1), CancellationToken.None);

        Assert.False(File.Exists(Path.Combine(_directory, "unknown.bin.part")));
    }

    public void Dispose()
    {
        try { Directory.Delete(_directory, recursive: true); } catch { }
    }
}
