using System.Text.Json;
using MomiMpRelay.Configuration;
using MomiMpRelay.FileSystem;
using MomiMpRelay.Modes;
using MomiMpRelay.Models;
using MomiMpRelay.Status;

namespace MomiMpRelay;

static class Program
{
    const int DefaultPort = 7777;

    static async Task<int> Main(string[] args)
    {
        bool noArgs = args.Length == 0;
        string firstArg = noArgs ? "" : args[0].ToLowerInvariant();
        if (firstArg is "help" or "-h" or "--help" or "/?") { PrintUsage(); return 0; }

        bool firstIsFlag = !noArgs && args[0].StartsWith('-');
        string mode = (noArgs || firstIsFlag) ? "auto" : args[0].ToLowerInvariant();
        string? explicitDir = null;
        string? instanceId = null;
        string? connectHost = null;
        int port = DefaultPort;

        for (int i = firstIsFlag ? 0 : 1; i < args.Length; i++)
        {
            if (args[i] == "--port" && i + 1 < args.Length) port = int.Parse(args[++i]);
            else if (args[i] == "--dir" && i + 1 < args.Length) explicitDir = args[++i];
            else if (args[i] == "--instance" && i + 1 < args.Length) instanceId = args[++i];
            else if (mode == "join" && connectHost is null && !args[i].StartsWith('-')) connectHost = args[i];
        }

        string mpDir = explicitDir ?? (instanceId is not null
            ? RelayDirectories.InstanceDir(instanceId)
            : RelayDirectories.ResolveMpDir());
        Directory.CreateDirectory(mpDir);
        RelayFileStore.CleanupTemporaryFiles(mpDir);
        string outPath = Path.Combine(mpDir, "out.json");
        string remotePath = Path.Combine(mpDir, "remote.json");

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
        Console.WriteLine("┌─ MOMI Multiplayer Relay ─────────────────────────────────┐");
        Console.WriteLine($"│  mode : {mode,-51}│");
        Console.WriteLine($"│  port : {port,-51}│");
        Console.WriteLine($"│  dir  : {mpDir,-51}│");
        Console.WriteLine("└──────────────────────────────────────────────────────────┘");
        Console.WriteLine();

        var reporter = new StatusReporter(mpDir);
        var statusTask = Task.Run(() => reporter.RunAsync(cts.Token), cts.Token);
        var host = new RelayHost(port, mpDir, outPath, remotePath, reporter);
        var client = connectHost is null ? null : new RelayClient(connectHost, port, mpDir, outPath, remotePath, reporter);
        var auto = new AutoRelay(port, mpDir, remotePath, reporter, ReadControlAsync,
            (modePort, token) => new RelayHost(modePort, mpDir, outPath, remotePath, reporter).RunAsync(token),
            (hostName, modePort, token) => new RelayClient(hostName, modePort, mpDir, outPath, remotePath, reporter).RunAsync(token));

        int exitCode = 0;
        bool errored = false;
        try
        {
            exitCode = mode switch
            {
                "auto" => await auto.RunAsync(cts.Token),
                "host" => await host.RunAsync(cts.Token),
                "join" when client is not null => await client.RunAsync(cts.Token),
                "join" => Err("Specify host IP.  Example: MomiMpRelay join 192.168.1.5"),
                _ => Err($"Unknown mode '{mode}'."),
            };
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) { errored = true; exitCode = 1; Console.Error.WriteLine($"Fatal: {ex.Message}"); }
        finally
        {
            try { File.Delete(remotePath); } catch { }
            cts.Cancel();
            try { await statusTask; } catch { }
            reporter.TryDelete();
            Console.WriteLine("[MOMI-MP] Stopped.");
        }

        if (noArgs && errored)
        {
            Console.WriteLine();
            Console.WriteLine("Press any key to close…");
            try { Console.ReadKey(true); } catch { }
        }
        return exitCode;
    }

    static async Task<RelayControl?> ReadControlAsync(string path, CancellationToken ct)
    {
        for (int attempt = 0; attempt < 6; attempt++)
        {
            try
            {
                await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read,
                    FileShare.ReadWrite | FileShare.Delete);
                return await JsonSerializer.DeserializeAsync<RelayControl>(fs, cancellationToken: ct);
            }
            catch (FileNotFoundException) { return null; }
            catch (DirectoryNotFoundException) { return null; }
            catch (JsonException) { return null; }
            catch (IOException) { await Task.Delay(15, ct); }
            catch (UnauthorizedAccessException) { await Task.Delay(15, ct); }
        }
        return null;
    }

    static void PrintUsage()
    {
        Console.WriteLine("Usage:");
        Console.WriteLine("  MomiMpRelay");
        Console.WriteLine("  MomiMpRelay auto [--port N] [--dir <path>]");
        Console.WriteLine("  MomiMpRelay host [--port N] [--dir <path>]");
        Console.WriteLine("  MomiMpRelay join <ip> [--port N] [--dir <path>]");
        Console.WriteLine();
        Console.WriteLine("Default port : " + DefaultPort);
    }

    static int Err(string message) { Console.Error.WriteLine("Error: " + message); PrintUsage(); return 1; }
}
