using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace MomiMpRelay.Tests;

public sealed class RelayIntegrationTests
{
    [Fact(Timeout = 60000)]
    public async Task HostAndClientExchangeStateAndClientCanReconnect()
    {
        using var workspace = new TestWorkspace();
        var port = GetFreePort();
        using var host = RelayProcess.Start("host", port, workspace.HostDirectory);
        Assert.True(await WaitUntilAsync(
            () => File.Exists(Path.Combine(workspace.HostDirectory, "mp_status.json")),
            TimeSpan.FromSeconds(10)));

        using var client = RelayProcess.Start("join", port, workspace.ClientDirectory, "127.0.0.1");
        var snapshotRequest = Path.Combine(workspace.HostDirectory, "mp_snap_request");
        Assert.True(await WaitUntilAsync(() => File.Exists(snapshotRequest), TimeSpan.FromSeconds(10)));
        await File.WriteAllBytesAsync(Path.Combine(workspace.HostDirectory, "world_snapshot.json"), []);
        await File.WriteAllTextAsync(Path.Combine(workspace.HostDirectory, "mp_snap_ready"), "ready");
        await WriteStateAsync(workspace.HostDirectory, "host-player");
        await WriteStateAsync(workspace.ClientDirectory, "client-player");

        Assert.True(await WaitUntilAsync(
            () => ContainsPlayerAsync(Path.Combine(workspace.HostDirectory, "remote.json"), "client-player"),
            TimeSpan.FromSeconds(15)), host.Output + client.Output);
        Assert.True(await WaitUntilAsync(
            () => ContainsPlayerAsync(Path.Combine(workspace.ClientDirectory, "remote.json"), "host-player"),
            TimeSpan.FromSeconds(15)));

        client.Stop();
        Assert.True(await WaitUntilAsync(
            () => StatusHasPeersAsync(Path.Combine(workspace.HostDirectory, "mp_status.json"), 0),
            TimeSpan.FromSeconds(10)));
        using var reconnectedClient = RelayProcess.Start("join", port, workspace.ClientDirectory, "127.0.0.1");
        Assert.True(await WaitUntilAsync(
            () => StatusHasPeersAsync(Path.Combine(workspace.HostDirectory, "mp_status.json"), 1),
            TimeSpan.FromSeconds(15)));
    }

    [Fact(Timeout = 60000)]
    public async Task ClientReceivesCompleteBinaryWorldSnapshot()
    {
        using var workspace = new TestWorkspace();
        var port = GetFreePort();
        using var host = RelayProcess.Start("host", port, workspace.HostDirectory);
        using var client = RelayProcess.Start("join", port, workspace.ClientDirectory, "127.0.0.1");

        var requestPath = Path.Combine(workspace.HostDirectory, "mp_snap_request");
        Assert.True(await WaitUntilAsync(() => File.Exists(requestPath), TimeSpan.FromSeconds(15)),
            host.Output + client.Output);

        var world = Encoding.UTF8.GetBytes("{\"world\":\"integration-test\"}");
        var terrain = Enumerable.Range(0, 4097).Select(value => (byte)(value % 251)).ToArray();
        await File.WriteAllBytesAsync(Path.Combine(workspace.HostDirectory, "world_snapshot.json"), world);
        await File.WriteAllBytesAsync(Path.Combine(workspace.HostDirectory, "world_farm_terrain.bin"), terrain);
        await File.WriteAllTextAsync(Path.Combine(workspace.HostDirectory, "mp_snap_ready"), "ready");

        var clientWorldPath = Path.Combine(workspace.ClientDirectory, "world_snapshot.json");
        var clientTerrainPath = Path.Combine(workspace.ClientDirectory, "world_farm_terrain.bin");
        Assert.True(await WaitUntilAsync(
            () => FilesEqualAsync(clientWorldPath, world), TimeSpan.FromSeconds(20)),
            host.Output + client.Output);
        Assert.True(await WaitUntilAsync(
            () => FilesEqualAsync(clientTerrainPath, terrain), TimeSpan.FromSeconds(20)));
        Assert.True(await WaitUntilAsync(
            () => File.Exists(Path.Combine(workspace.ClientDirectory, "mp_apply_world")),
            TimeSpan.FromSeconds(10)));
    }

    static async Task WriteStateAsync(string directory, string playerId)
    {
        await File.WriteAllTextAsync(
            Path.Combine(directory, "out.json"),
            $"{{\"player_id\":\"{playerId}\",\"tick\":1}}");
    }

    static async Task<bool> ContainsPlayerAsync(string path, string playerId)
    {
        if (!File.Exists(path)) return false;
        try { return (await File.ReadAllTextAsync(path)).Contains(playerId, StringComparison.Ordinal); }
        catch (IOException) { return false; }
    }

    static async Task<bool> StatusHasPeersAsync(string path, int peers)
    {
        if (!File.Exists(path)) return false;
        try
        {
            var json = await File.ReadAllTextAsync(path);
            return System.Text.Json.JsonDocument.Parse(json).RootElement
                .GetProperty("peers").GetInt32() == peers;
        }
        catch { return false; }
    }

    static async Task<bool> FilesEqualAsync(string path, byte[] expected)
    {
        if (!File.Exists(path)) return false;
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
            if (await condition()) return true;
            await Task.Delay(50);
        }
        return await condition();
    }

    static Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout) =>
        WaitUntilAsync(() => Task.FromResult(condition()), timeout);

    static int GetFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    sealed class TestWorkspace : IDisposable
    {
        readonly string _root = Path.Combine(Path.GetTempPath(), "MomiMpRelay.Tests", Guid.NewGuid().ToString("N"));
        public string HostDirectory { get; }
        public string ClientDirectory { get; }

        public TestWorkspace()
        {
            HostDirectory = Path.Combine(_root, "host");
            ClientDirectory = Path.Combine(_root, "client");
            Directory.CreateDirectory(HostDirectory);
            Directory.CreateDirectory(ClientDirectory);
        }

        public void Dispose()
        {
            try { Directory.Delete(_root, recursive: true); } catch { }
        }
    }

    sealed class RelayProcess : IDisposable
    {
        readonly Process _process;
        readonly StringBuilder _output = new();

        RelayProcess(Process process)
        {
            _process = process;
            _process.OutputDataReceived += (_, args) =>
            {
                if (args.Data is not null) lock (_output) _output.AppendLine(args.Data);
            };
            _process.ErrorDataReceived += (_, args) =>
            {
                if (args.Data is not null) lock (_output) _output.AppendLine(args.Data);
            };
        }

        public string Output
        {
            get { lock (_output) return _output.ToString(); }
        }

        public static RelayProcess Start(string mode, int port, string directory, string? host = null)
        {
            var assembly = typeof(MomiMpRelay.Models.RelayPacket).Assembly.Location;
            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "dotnet",
                    UseShellExecute = false,
                    CreateNoWindow = true,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                },
            };
            process.StartInfo.ArgumentList.Add(assembly);
            process.StartInfo.ArgumentList.Add(mode);
            if (host is not null) process.StartInfo.ArgumentList.Add(host);
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
            if (_process.HasExited) return;
            try { _process.Kill(entireProcessTree: true); } catch (InvalidOperationException) { }
            _process.WaitForExit(5000);
        }

        public void Dispose() => Stop();
    }
}
