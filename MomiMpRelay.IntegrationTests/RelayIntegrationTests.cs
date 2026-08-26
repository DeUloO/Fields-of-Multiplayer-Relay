using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace MomiMpRelay.IntegrationTests;

public sealed class RelayIntegrationTests
{
    [Fact(Timeout = 60000)]
    public async Task HostAndClientExchangeStateAndClientCanReconnect()
    {
        using var workspace = new TestWorkspace();
        var port = GetFreePort();
        using var host = RelayProcess.Start("host", port, workspace.HostDirectory);
        Assert.True(await WaitUntilAsync(() => File.Exists(Path.Combine(workspace.HostDirectory, "mp_status.json")), TimeSpan.FromSeconds(10)));
        using var client = RelayProcess.Start("join", port, workspace.ClientDirectory, "127.0.0.1");
        await CompleteEmptySnapshotAsync(workspace.HostDirectory);
        await WriteStateAsync(workspace.HostDirectory, "host-player");
        await WriteStateAsync(workspace.ClientDirectory, "client-player");
        Assert.True(await WaitUntilAsync(() => ContainsPlayerAsync(Path.Combine(workspace.HostDirectory, "remote.json"), "client-player"), TimeSpan.FromSeconds(15)), host.Output + client.Output);
        Assert.True(await WaitUntilAsync(() => ContainsPlayerAsync(Path.Combine(workspace.ClientDirectory, "remote.json"), "host-player"), TimeSpan.FromSeconds(15)));
        client.Stop();
        Assert.True(await WaitUntilAsync(() => StatusHasPeersAsync(Path.Combine(workspace.HostDirectory, "mp_status.json"), 0), TimeSpan.FromSeconds(10)));
        using var reconnectedClient = RelayProcess.Start("join", port, workspace.ClientDirectory, "127.0.0.1");
        Assert.True(await WaitUntilAsync(() => StatusHasPeersAsync(Path.Combine(workspace.HostDirectory, "mp_status.json"), 1), TimeSpan.FromSeconds(15)));
    }

    [Fact(Timeout = 60000)]
    public async Task ClientReceivesCompleteBinaryWorldSnapshot()
    {
        using var workspace = new TestWorkspace();
        var port = GetFreePort();
        using var host = RelayProcess.Start("host", port, workspace.HostDirectory);
        await WriteStateAsync(workspace.HostDirectory, "host-player");
        using var client = RelayProcess.Start("join", port, workspace.ClientDirectory, "127.0.0.1");
        var requestPath = Path.Combine(workspace.HostDirectory, "mp_snap_request");
        Assert.True(await WaitUntilAsync(() => File.Exists(requestPath), TimeSpan.FromSeconds(15)), host.Output + client.Output);
        var world = Encoding.UTF8.GetBytes("{\"world\":\"integration-test\"}");
        var terrain = Enumerable.Range(0, 4097).Select(value => (byte)(value % 251)).ToArray();
        await File.WriteAllBytesAsync(Path.Combine(workspace.HostDirectory, "world_snapshot.json"), world);
        await File.WriteAllBytesAsync(Path.Combine(workspace.HostDirectory, "world_farm_terrain.bin"), terrain);
        await File.WriteAllTextAsync(Path.Combine(workspace.HostDirectory, "mp_snap_ready"), "ready");
        Assert.True(await WaitUntilAsync(() => FilesEqualAsync(Path.Combine(workspace.ClientDirectory, "world_snapshot.json"), world), TimeSpan.FromSeconds(20)), host.Output + client.Output);
        Assert.True(await WaitUntilAsync(() => FilesEqualAsync(Path.Combine(workspace.ClientDirectory, "world_farm_terrain.bin"), terrain), TimeSpan.FromSeconds(20)));
        Assert.True(await WaitUntilAsync(() => File.Exists(Path.Combine(workspace.ClientDirectory, "mp_apply_world")), TimeSpan.FromSeconds(10)));
    }

    [Fact(Timeout = 60000)]
    public async Task HostTracksMultipleClients()
    {
        using var workspace = new TestWorkspace();
        var port = GetFreePort();
        using var host = RelayProcess.Start("host", port, workspace.HostDirectory);
        await WriteStateAsync(workspace.HostDirectory, "host-player");
        using var clientOne = RelayProcess.Start("join", port, workspace.ClientDirectory, "127.0.0.1");
        await CompleteEmptySnapshotAsync(workspace.HostDirectory);
        var secondDirectory = workspace.CreateAdditionalDirectory("client-two");
        try
        {
            File.Delete(Path.Combine(workspace.HostDirectory, "mp_snap_request"));
        }
        catch { }
        using var clientTwo = RelayProcess.Start("join", port, secondDirectory, "127.0.0.1");
        await CompleteEmptySnapshotAsync(workspace.HostDirectory);
        await Task.WhenAll(
            WriteStateAsync(workspace.ClientDirectory, "client-one"),
            WriteStateAsync(secondDirectory, "client-two"));
        Assert.True(await WaitUntilAsync(() => ContainsPlayerAsync(Path.Combine(workspace.HostDirectory, "remote.json"), "client-one"), TimeSpan.FromSeconds(15)));
        Assert.True(await WaitUntilAsync(() => ContainsPlayerAsync(Path.Combine(workspace.HostDirectory, "remote.json"), "client-two"), TimeSpan.FromSeconds(15)));
    }

    [Fact(Timeout = 60000)]
    public async Task ClientReportsReconnectAfterHostShutdown()
    {
        using var workspace = new TestWorkspace();
        var port = GetFreePort();
        using var host = RelayProcess.Start("host", port, workspace.HostDirectory);
        await WriteStateAsync(workspace.HostDirectory, "host-player");
        using var client = RelayProcess.Start("join", port, workspace.ClientDirectory, "127.0.0.1");
        await CompleteEmptySnapshotAsync(workspace.HostDirectory);
        host.Stop();
        Assert.True(await WaitUntilAsync(() => Task.FromResult(client.Output.Contains("Reconnecting", StringComparison.Ordinal)), TimeSpan.FromSeconds(40)), client.Output);
    }

    [Fact(Timeout = 60000)]
    public async Task ClientShutdownDuringSnapshotDoesNotLeaveProcessRunning()
    {
        using var workspace = new TestWorkspace();
        var port = GetFreePort();
        using var host = RelayProcess.Start("host", port, workspace.HostDirectory);
        await WriteStateAsync(workspace.HostDirectory, "host-player");
        using var client = RelayProcess.Start("join", port, workspace.ClientDirectory, "127.0.0.1");
        Assert.True(await WaitUntilAsync(() => File.Exists(Path.Combine(workspace.HostDirectory, "mp_snap_request")), TimeSpan.FromSeconds(15)));
        client.Stop();
        Assert.True(await WaitUntilAsync(() => client.HasExited, TimeSpan.FromSeconds(5)));
        Assert.False(File.Exists(Path.Combine(workspace.ClientDirectory, "mp_apply_world")));
    }

    [Fact(Timeout = 60000)]
    public async Task RapidStateUpdatesConvergeToLatestState()
    {
        using var workspace = new TestWorkspace();
        var port = GetFreePort();
        using var host = RelayProcess.Start("host", port, workspace.HostDirectory);
        await WriteStateAsync(workspace.HostDirectory, "host-player");
        using var client = RelayProcess.Start("join", port, workspace.ClientDirectory, "127.0.0.1");
        await CompleteEmptySnapshotAsync(workspace.HostDirectory);
        for (var tick = 0; tick < 100; tick++)
            await File.WriteAllTextAsync(Path.Combine(workspace.ClientDirectory, "out.json"), $"{{\"player_id\":\"rapid-player\",\"tick\":{tick}}}");
        Assert.True(await WaitUntilAsync(() => ContainsTextAsync(Path.Combine(workspace.HostDirectory, "remote.json"), "rapid-player"), TimeSpan.FromSeconds(15)));
        Assert.True(await WaitUntilAsync(() => HasPlayerTickAsync(Path.Combine(workspace.HostDirectory, "remote.json"), "rapid-player", 99), TimeSpan.FromSeconds(15)));
    }

    static async Task WriteStateAsync(string directory, string playerId) => await File.WriteAllTextAsync(Path.Combine(directory, "out.json"), $"{{\"player_id\":\"{playerId}\",\"tick\":1}}");
    static async Task CompleteEmptySnapshotAsync(string directory)
    {
        var request = Path.Combine(directory, "mp_snap_request");
        Assert.True(await WaitUntilAsync(() => File.Exists(request), TimeSpan.FromSeconds(15)));
        await File.WriteAllBytesAsync(Path.Combine(directory, "world_snapshot.json"), []);
        await File.WriteAllTextAsync(Path.Combine(directory, "mp_snap_ready"), "ready");
    }
    static async Task<bool> ContainsTextAsync(string path, string value)
    {
        if (!File.Exists(path))
            return false;
        try
        {
            return (await File.ReadAllTextAsync(path)).Contains(value, StringComparison.Ordinal);
        }
        catch (IOException) { return false; }
    }
    static Task<bool> ContainsPlayerAsync(string path, string value) => ContainsTextAsync(path, value);
    static async Task<bool> HasPlayerTickAsync(string path, string playerId, int tick)
    {
        if (!File.Exists(path))
            return false;
        try
        {
            using var document = System.Text.Json.JsonDocument.Parse(await File.ReadAllTextAsync(path));
            foreach (var player in document.RootElement.GetProperty("players").EnumerateArray())
                if (player.GetProperty("player_id").GetString() == playerId && player.GetProperty("tick").GetInt32() == tick)
                    return true;
        }
        catch { }
        return false;
    }
    static async Task<bool> StatusHasPeersAsync(string path, int peers)
    {
        if (!File.Exists(path))
            return false;
        try
        {
            return System.Text.Json.JsonDocument.Parse(await File.ReadAllTextAsync(path)).RootElement.GetProperty("peers").GetInt32() == peers;
        }
        catch { return false; }
    }
    static async Task<bool> FilesEqualAsync(string path, byte[] expected)
    {
        if (!File.Exists(path))
            return false;
        try
        {
            var actual = await File.ReadAllBytesAsync(path);
            return actual.Length == expected.Length && actual.AsSpan().SequenceEqual(expected);
        }
        catch (IOException) { return false; }
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
    static Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout) => WaitUntilAsync(() => Task.FromResult(condition()), timeout);
    static int GetFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    sealed class TestWorkspace : IDisposable
    {
        readonly string _root = Path.Combine(Path.GetTempPath(), "MomiMpRelay.Tests", Guid.NewGuid().ToString("N"));
        public string HostDirectory { get; } = "";
        public string ClientDirectory { get; } = "";
        public TestWorkspace()
        {
            HostDirectory = Path.Combine(_root, "host");
            ClientDirectory = Path.Combine(_root, "client");
            Directory.CreateDirectory(HostDirectory);
            Directory.CreateDirectory(ClientDirectory);
        }
        public string CreateAdditionalDirectory(string name)
        {
            var path = Path.Combine(_root, name);
            Directory.CreateDirectory(path);
            return path;
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
        readonly Process _process; readonly StringBuilder _output = new();
        RelayProcess(Process process)
        {
            _process = process;
            _process.OutputDataReceived += (_, e) => { if (e.Data is not null) lock (_output) _output.AppendLine(e.Data); };
            _process.ErrorDataReceived += (_, e) => { if (e.Data is not null) lock (_output) _output.AppendLine(e.Data); };
        }
        public string Output
        {
            get
            {
                lock (_output)
                    return _output.ToString();
            }
        }
        public bool HasExited => _process.HasExited;
        public static RelayProcess Start(string mode, int port, string directory, string? host = null)
        {
            var process = new Process { StartInfo = new ProcessStartInfo { FileName = "dotnet", UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true } };
            process.StartInfo.ArgumentList.Add(typeof(MomiMpRelay.Models.RelayPacket).Assembly.Location);
            process.StartInfo.ArgumentList.Add(mode);
            if (host is not null)
                process.StartInfo.ArgumentList.Add(host);
            process.StartInfo.ArgumentList.Add("--port");
            process.StartInfo.ArgumentList.Add(port.ToString());
            process.StartInfo.ArgumentList.Add("--dir");
            process.StartInfo.ArgumentList.Add(directory);
            var relay = new RelayProcess(process);
            Assert.True(process.Start());
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            return relay;
        }
        public void Stop()
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
        public void Dispose() => Stop();
    }
}
