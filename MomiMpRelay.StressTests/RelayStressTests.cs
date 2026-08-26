using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;

namespace MomiMpRelay.StressTests;

public sealed class RelayStressTests
{
    [Trait("Category", "Stress")]
    [Fact(Timeout = 180000)]
    public async Task MultipleClientsSurviveRapidStateUpdates()
    {
        var clientCount = ReadSetting("MOMI_STRESS_CLIENTS", 4, 1, 20);
        var updateCount = ReadSetting("MOMI_STRESS_UPDATES", 500, 10, 10000);
        using var workspace = new StressWorkspace(clientCount);
        var port = GetFreePort();
        using var host = RelayProcess.Start("host", port, workspace.HostDirectory);
        await WriteStateAsync(workspace.HostDirectory, "host-player", 0);

        var clients = new List<RelayProcess>();
        try
        {
            for (var index = 0; index < clientCount; index++)
            {
                var directory = workspace.ClientDirectories[index];
                try
                {
                    File.Delete(Path.Combine(workspace.HostDirectory, "mp_snap_request"));
                }
                catch { }
                var client = RelayProcess.Start("join", port, directory, "127.0.0.1");
                clients.Add(client);
                await CompleteEmptySnapshotAsync(workspace.HostDirectory);
            }

            await Task.WhenAll(clients.Select((_, index) => SendUpdatesAsync(
                workspace.ClientDirectories[index], $"stress-player-{index}", updateCount)));

            Assert.True(await WaitUntilAsync(
                () => HasAllPlayersAsync(Path.Combine(workspace.HostDirectory, "remote.json"), clientCount),
                TimeSpan.FromSeconds(30)));
        }
        finally
        {
            foreach (var client in clients)
                client.Dispose();
        }
    }

    [Trait("Category", "Stress")]
    [Fact(Timeout = 240000)]
    public async Task RepeatedClientReconnectsRemainHealthy()
    {
        var cycles = ReadSetting("MOMI_STRESS_RECONNECTS", 5, 2, 20);
        using var workspace = new StressWorkspace(1);
        var port = GetFreePort();
        using var host = RelayProcess.Start("host", port, workspace.HostDirectory);
        await WriteStateAsync(workspace.HostDirectory, "host-player", 0);

        for (var cycle = 0; cycle < cycles; cycle++)
        {
            try
            {
                File.Delete(Path.Combine(workspace.HostDirectory, "mp_snap_request"));
            }
            catch { }
            using var client = RelayProcess.Start("join", port, workspace.ClientDirectories[0], "127.0.0.1");
            await CompleteEmptySnapshotAsync(workspace.HostDirectory);
            client.Dispose();
            Assert.True(await WaitUntilAsync(
                () => HasPeerCountAsync(Path.Combine(workspace.HostDirectory, "mp_status.json"), 0),
                TimeSpan.FromSeconds(20)));
        }
    }

    [Trait("Category", "Stress")]
    [Fact(Timeout = 240000)]
    public async Task HigherClientCountSurvivesSustainedUpdates()
    {
        var clientCount = ReadSetting("MOMI_STRESS_HIGH_CLIENTS", 8, 2, 20);
        var updateCount = ReadSetting("MOMI_STRESS_HIGH_UPDATES", 2000, 100, 20000);
        using var workspace = new StressWorkspace(clientCount);
        var port = GetFreePort();
        using var host = RelayProcess.Start("host", port, workspace.HostDirectory);
        await WriteStateAsync(workspace.HostDirectory, "host-player", 0);
        var clients = new List<RelayProcess>();
        try
        {
            for (var index = 0; index < clientCount; index++)
            {
                try
                {
                    File.Delete(Path.Combine(workspace.HostDirectory, "mp_snap_request"));
                }
                catch { }
                var client = RelayProcess.Start("join", port, workspace.ClientDirectories[index], "127.0.0.1");
                clients.Add(client);
                await CompleteEmptySnapshotAsync(workspace.HostDirectory);
            }

            await Task.WhenAll(clients.Select((_, index) => SendUpdatesAsync(
                workspace.ClientDirectories[index], $"high-player-{index}", updateCount)));
            Assert.True(await WaitUntilAsync(
                () => HasNamedPlayersAsync(Path.Combine(workspace.HostDirectory, "remote.json"), "high-player-", clientCount),
                TimeSpan.FromSeconds(45)));
        }
        finally
        {
            foreach (var client in clients)
                client.Dispose();
        }
    }

    [Trait("Category", "Stress")]
    [Fact(Timeout = 180000)]
    public async Task SlowFilesystemConsumersDoNotStopHostUpdates()
    {
        var clientCount = ReadSetting("MOMI_STRESS_SLOW_CLIENTS", 4, 1, 12);
        var updateCount = ReadSetting("MOMI_STRESS_SLOW_UPDATES", 500, 50, 5000);
        using var workspace = new StressWorkspace(clientCount);
        var port = GetFreePort();
        using var host = RelayProcess.Start("host", port, workspace.HostDirectory);
        await WriteStateAsync(workspace.HostDirectory, "host-player", 0);
        var clients = new List<RelayProcess>();
        try
        {
            for (var index = 0; index < clientCount; index++)
            {
                try
                {
                    File.Delete(Path.Combine(workspace.HostDirectory, "mp_snap_request"));
                }
                catch { }
                var client = RelayProcess.Start("join", port, workspace.ClientDirectories[index], "127.0.0.1");
                clients.Add(client);
                await CompleteEmptySnapshotAsync(workspace.HostDirectory);
            }

            var updateTask = SendUpdatesAsync(workspace.ClientDirectories[0], "slow-source", updateCount);
            var lockTasks = workspace.ClientDirectories.Select(directory =>
                LockRemoteFileRepeatedlyAsync(directory, TimeSpan.FromSeconds(8))).ToArray();
            await Task.WhenAll(updateTask, Task.WhenAll(lockTasks));

            Assert.True(await WaitUntilAsync(
                () => HasNamedPlayersAsync(Path.Combine(workspace.HostDirectory, "remote.json"), "slow-source", 1),
                TimeSpan.FromSeconds(30)));
        }
        finally
        {
            foreach (var client in clients)
                client.Dispose();
        }
    }

    [Trait("Category", "Stress")]
    [Fact(Timeout = 300000)]
    public async Task RelayRemainsHealthyDuringSoakPeriod()
    {
        var seconds = ReadSetting("MOMI_STRESS_SOAK_SECONDS", 30, 10, 300);
        using var workspace = new StressWorkspace(2);
        var port = GetFreePort();
        using var host = RelayProcess.Start("host", port, workspace.HostDirectory);
        await WriteStateAsync(workspace.HostDirectory, "host-player", 0);
        var clients = new List<RelayProcess>();
        try
        {
            for (var index = 0; index < 2; index++)
            {
                try
                {
                    File.Delete(Path.Combine(workspace.HostDirectory, "mp_snap_request"));
                }
                catch { }
                var client = RelayProcess.Start("join", port, workspace.ClientDirectories[index], "127.0.0.1");
                clients.Add(client);
                await CompleteEmptySnapshotAsync(workspace.HostDirectory);
            }

            var end = DateTime.UtcNow.AddSeconds(seconds);
            var tick = 0;
            while (DateTime.UtcNow < end)
            {
                await Task.WhenAll(
                    WriteStateAsync(workspace.ClientDirectories[0], "soak-player-0", tick),
                    WriteStateAsync(workspace.ClientDirectories[1], "soak-player-1", tick));
                tick++;
                await Task.Delay(50);
            }

            Assert.True(await WaitUntilAsync(
                () => HasNamedPlayersAsync(Path.Combine(workspace.HostDirectory, "remote.json"), "soak-player-", 2),
                TimeSpan.FromSeconds(30)));
            Assert.All(clients, client => Assert.False(client.HasExited));
        }
        finally
        {
            foreach (var client in clients)
                client.Dispose();
        }
    }

    static async Task SendUpdatesAsync(string directory, string playerId, int count)
    {
        var path = Path.Combine(directory, "out.json");
        for (var tick = 0; tick < count; tick++)
            await File.WriteAllTextAsync(path, $"{{\"player_id\":\"{playerId}\",\"tick\":{tick}}}");
    }

    static async Task LockRemoteFileRepeatedlyAsync(string directory, TimeSpan duration)
    {
        var path = Path.Combine(directory, "remote.json");
        var end = DateTime.UtcNow + duration;
        while (DateTime.UtcNow < end)
        {
            if (File.Exists(path))
            {
                try
                {
                    await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read,
                        FileShare.None);
                    await Task.Delay(100);
                }
                catch (IOException) { }
            }
            await Task.Delay(50);
        }
    }

    static async Task WriteStateAsync(string directory, string playerId, int tick) =>
        await File.WriteAllTextAsync(Path.Combine(directory, "out.json"),
            $"{{\"player_id\":\"{playerId}\",\"tick\":{tick}}}");

    static async Task CompleteEmptySnapshotAsync(string directory)
    {
        var request = Path.Combine(directory, "mp_snap_request");
        Assert.True(await WaitUntilAsync(() => File.Exists(request), TimeSpan.FromSeconds(20)));
        await File.WriteAllBytesAsync(Path.Combine(directory, "world_snapshot.json"), []);
        await File.WriteAllTextAsync(Path.Combine(directory, "mp_snap_ready"), "ready");
    }

    static async Task<bool> HasAllPlayersAsync(string path, int clientCount)
    {
        if (!File.Exists(path))
            return false;
        try
        {
            using var document = JsonDocument.Parse(await File.ReadAllTextAsync(path));
            var players = document.RootElement.GetProperty("players");
            return Enumerable.Range(0, clientCount).All(index =>
                players.EnumerateArray().Any(player =>
                    player.GetProperty("player_id").GetString() == $"stress-player-{index}"));
        }
        catch { return false; }
    }

    static async Task<bool> HasNamedPlayersAsync(string path, string prefix, int count)
    {
        if (!File.Exists(path))
            return false;
        try
        {
            using var document = JsonDocument.Parse(await File.ReadAllTextAsync(path));
            return document.RootElement.GetProperty("players").EnumerateArray()
                .Count(player => player.GetProperty("player_id").GetString()?.StartsWith(prefix, StringComparison.Ordinal) == true) == count;
        }
        catch { return false; }
    }

    static async Task<bool> HasPeerCountAsync(string path, int expected)
    {
        if (!File.Exists(path))
            return false;
        try
        {
            return JsonDocument.Parse(await File.ReadAllTextAsync(path)).RootElement.GetProperty("peers").GetInt32() == expected;
        }
        catch { return false; }
    }

    static async Task<bool> WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
                return true;
            await Task.Delay(50);
        }
        return await condition();
    }

    static Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout) =>
        WaitUntilAsync(() => Task.FromResult(condition()), timeout);

    static int ReadSetting(string name, int fallback, int minimum, int maximum)
    {
        return int.TryParse(Environment.GetEnvironmentVariable(name), out var value)
            ? Math.Clamp(value, minimum, maximum)
            : fallback;
    }

    static int GetFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    sealed class StressWorkspace : IDisposable
    {
        readonly string _root = Path.Combine(Path.GetTempPath(), "MomiMpRelay.StressTests", Guid.NewGuid().ToString("N"));
        public string HostDirectory
        {
            get;
        }
        public IReadOnlyList<string> ClientDirectories
        {
            get;
        }

        public StressWorkspace(int clientCount)
        {
            HostDirectory = Path.Combine(_root, "host");
            Directory.CreateDirectory(HostDirectory);
            var directories = new List<string>();
            for (var index = 0; index < clientCount; index++)
            {
                var directory = Path.Combine(_root, $"client-{index}");
                Directory.CreateDirectory(directory);
                directories.Add(directory);
            }
            ClientDirectories = directories;
        }

        public void Dispose()
        {
            try
            {
                Directory.Delete(_root, true);
            }
            catch { }
        }
    }

    sealed class RelayProcess : IDisposable
    {
        readonly Process _process;

        RelayProcess(Process process) => _process = process;

        public bool HasExited => _process.HasExited;

        public static RelayProcess Start(string mode, int port, string directory, string? host = null)
        {
            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "dotnet",
                    UseShellExecute = false,
                    CreateNoWindow = true,
                },
            };
            process.StartInfo.ArgumentList.Add(typeof(MomiMpRelay.Models.RelayPacket).Assembly.Location);
            process.StartInfo.ArgumentList.Add(mode);
            if (host is not null)
                process.StartInfo.ArgumentList.Add(host);
            process.StartInfo.ArgumentList.Add("--port");
            process.StartInfo.ArgumentList.Add(port.ToString());
            process.StartInfo.ArgumentList.Add("--dir");
            process.StartInfo.ArgumentList.Add(directory);
            Assert.True(process.Start());
            return new RelayProcess(process);
        }

        public void Dispose()
        {
            if (_process.HasExited)
                return;
            try
            {
                _process.Kill(true);
            }
            catch (InvalidOperationException) { }
            _process.WaitForExit(5000);
        }
    }
}
