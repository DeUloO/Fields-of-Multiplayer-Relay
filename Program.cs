using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Channels;

namespace MomiMpRelay;

sealed class ClientSession
{
    public readonly TcpClient     Tcp;
    public readonly NetworkStream Stream;
    public volatile string?       PlayerId;

        new BoundedChannelOptions(2) { FullMode = BoundedChannelFullMode.DropOldest });

    public readonly SemaphoreSlim WriteLock = new(1, 1);

    public ClientSession(TcpClient tcp) { Tcp = tcp; Stream = tcp.GetStream(); }

    public void Push(string json) => Outbox.Writer.TryWrite(json);
}

sealed class StatusReporter
{
    readonly string _path;
    long   _hb;
    string _state  = "idle";
    string _role   = "off";
    int    _peers;
    string? _detail;

    public StatusReporter(string mpDir) => _path = Path.Combine(mpDir, "mp_status.json");

    public void Set(string state, string role, int peers, string? detail = null)
    {
        _state = state; _role = role; _peers = peers; _detail = detail;
    }
    public void SetState(string state, string? detail = null) { _state = state; _detail = detail; }
    public void SetPeers(int peers) => _peers = peers;

    public async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await WriteOnceAsync(ct);
            try { await Task.Delay(500, ct); } catch { break; }
        }
    }

    public async Task WriteOnceAsync(CancellationToken ct)
    {
        var hb  = Interlocked.Increment(ref _hb);
        var obj = new JsonObject
        {
            ["hb"]     = hb,
            ["state"]  = _state,
            ["role"]   = _role,
            ["peers"]  = _peers,
            ["detail"] = _detail,
        };
        try
        {
            var tmp = _path + ".tmp";
            await File.WriteAllTextAsync(tmp, obj.ToJsonString(), ct);
            File.Move(tmp, _path, overwrite: true);
        }
        catch { /* transient IO — the next heartbeat will retry */ }
    }

    public void TryDelete() { try { File.Delete(_path); } catch { } }
}


sealed class SnapshotReceiver
{
    readonly string _mpDir;
    readonly Dictionary<string, FileStream> _open = new();

    public SnapshotReceiver(string mpDir) { _mpDir = mpDir; }

    public async Task HandleAsync(string kind, JsonObject msg, CancellationToken ct)
    {
        switch (kind)
        {
            case "snap_begin":
            {
                var name = msg["name"]?.GetValue<string>();
                if (name is null) break;
                CloseOne(name);
                var part = Path.Combine(_mpDir, name + ".part");
                _open[name] = new FileStream(part, FileMode.Create, FileAccess.Write,
                    FileShare.None, 1 << 16, useAsync: true);
                Console.WriteLine($"[CLIENT] Receiving {name} ({msg["bytes"]} bytes)…");
                break;
            }
            case "snap_chunk":
            {
                var name = msg["name"]?.GetValue<string>();
                var data = msg["data"]?.GetValue<string>();
                if (name is null || data is null) break;
                if (_open.TryGetValue(name, out var fs))
                {
                    var bytes = Convert.FromBase64String(data);
                    await fs.WriteAsync(bytes, ct);
                }
                break;
            }
            case "snap_end":
            {
                var name = msg["name"]?.GetValue<string>();
                if (name is null) break;
                CloseOne(name);
                var part  = Path.Combine(_mpDir, name + ".part");
                var final = Path.Combine(_mpDir, name);
                try { if (File.Exists(part)) File.Move(part, final, overwrite: true); }
                catch (Exception ex) { Console.WriteLine($"[CLIENT] snap_end {name}: {ex.Message}"); }
                break;
            }
            case "snap_done":
            {
                // Signal the game to adopt the just-delivered world.
                try
                {
                    await File.WriteAllTextAsync(Path.Combine(_mpDir, "mp_apply_world"), "", ct);
                    Console.WriteLine("[CLIENT] World snapshot complete → apply requested.");
                }
                catch (Exception ex) { Console.WriteLine($"[CLIENT] snap_done: {ex.Message}"); }
                break;
            }
        }
    }

    void CloseOne(string name)
    {
        if (_open.TryGetValue(name, out var fs))
        {
            try { fs.Dispose(); } catch { }
            _open.Remove(name);
        }
    }
}


static class Program
{
    const int DefaultPort = 7777;
    const int PollMs      = 50;    // poll out.json / control every 50 ms (~20 fps)

    // Base64 chars per snapshot chunk. Multiple of 4 so each chunk is a valid
    // standalone base64 segment. ~1 MB text/chunk keeps every frame well under
    // the 4 MB wire cap even after JSON envelope overhead.
    const int ChunkChars  = 1_000_000;

    // How long the host waits for its game to produce a snapshot (write
    // mp_snap_ready) after being asked. The game only serializes when in-world.
    const int SnapshotTimeoutMs = 20_000;

    static string FomRoot => Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "FieldsOfMistria");

    static string InstanceDir(string id)
    {
        var clean = new string(id.Where(c => char.IsLetterOrDigit(c) || c is '_' or '-').ToArray());
        if (clean.Length == 0 || clean.Equals("main", StringComparison.OrdinalIgnoreCase))
            return Path.Combine(FomRoot, "momi_mp");
        return Path.Combine(FomRoot, "momi_mp_" + clean);
    }

    static string ResolveMpDir()
    {
        var direct     = Path.Combine(FomRoot, "momi_mp");
        var candidates = new List<string> { direct };
        try
        {
            foreach (var sub in Directory.GetDirectories(FomRoot))
            {
                var mm = Path.Combine(sub, "momi_mp");   // e.g. FieldsOfMistria/beta/momi_mp
                if (Directory.Exists(mm)) { candidates.Add(mm); }
            }
        }
        catch { }

        string?  best     = null;
        DateTime bestTime = DateTime.MinValue;
        foreach (var c in candidates)
        {
            try
            {
                var ctrl = Path.Combine(c, "mp_control.json");
                if (File.Exists(ctrl))
                {
                    var t = File.GetLastWriteTimeUtc(ctrl);
                    if (t > bestTime) { bestTime = t; best = c; }
                }
            }
            catch { }
        }
        return best ?? direct;
    }

    static async Task<int> Main(string[] args)
    {
        bool   noArgs      = args.Length == 0;
        bool   firstIsFlag = !noArgs && args[0].StartsWith('-');
        string mode        = (noArgs || firstIsFlag) ? "auto" : args[0].ToLowerInvariant();

        if (mode is "help" or "-h" or "--help" or "/?") { PrintUsage(); return 0; }

        string? explicitDir = null;
        string? instanceId  = null;
        string? connectHost = null;
        int     port        = DefaultPort;

        for (int i = firstIsFlag ? 0 : 1; i < args.Length; i++)
        {
            if      (args[i] == "--port"     && i + 1 < args.Length) port        = int.Parse(args[++i]);
            else if (args[i] == "--dir"      && i + 1 < args.Length) explicitDir = args[++i];
            else if (args[i] == "--instance" && i + 1 < args.Length) instanceId  = args[++i];
            else if (mode == "join" && connectHost is null && !args[i].StartsWith('-'))
                connectHost = args[i];
        }

            explicitDir
            ?? (instanceId != null ? InstanceDir(instanceId) : ResolveMpDir());

        Directory.CreateDirectory(mpDir);

        string outPath    = Path.Combine(mpDir, "out.json");
        string remotePath = Path.Combine(mpDir, "remote.json");

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        Console.WriteLine("┌─ MOMI Multiplayer Relay ─────────────────────────────────┐");
        Console.WriteLine($"│  mode : {mode,-51}│");
        Console.WriteLine($"│  port : {port,-51}│");
        Console.WriteLine($"│  dir  : {mpDir,-51}│");
        Console.WriteLine("└──────────────────────────────────────────────────────────┘");
        Console.WriteLine();

        // One status reporter for the whole run; the game reads mp_status.json.
        var reporter   = new StatusReporter(mpDir);
        var statusTask = Task.Run(() => reporter.RunAsync(cts.Token), cts.Token);

        int  exitCode = 0;
        bool errored  = false;
        try
        {
            exitCode = mode switch
            {
                "auto" => await RunAutoAsync(port, mpDir, outPath, remotePath, reporter, cts.Token),
                "host" => await RunHostAsync(port, mpDir, outPath, remotePath, reporter, cts.Token),
                "join" when connectHost is not null
                       => await RunClientAsync(connectHost, port, mpDir, outPath, remotePath, reporter, cts.Token),
                "join" => Err("Specify host IP.  Example: MomiMpRelay join 192.168.1.5"),
                _      => Err($"Unknown mode '{mode}'."),
            };
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            errored = true;
            exitCode = 1;
            Console.Error.WriteLine($"Fatal: {ex.Message}");
        }
        finally
        {
            try { if (File.Exists(remotePath)) File.Delete(remotePath); } catch { }
            try { cts.Cancel(); } catch { }
            try { await statusTask; } catch { }
            reporter.TryDelete();   // absent file => game shows "Relay off"
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

    static void PrintUsage()
    {
        Console.WriteLine("Usage:");
        Console.WriteLine("  MomiMpRelay                                   (no args = auto; this is what double-clicking does)");
        Console.WriteLine("  MomiMpRelay auto [--port N] [--dir <path>]   (recommended: driven by the in-game Multiplayer tab)");
        Console.WriteLine("  MomiMpRelay host [--port N] [--dir <path>]");
        Console.WriteLine("  MomiMpRelay join <ip> [--port N] [--dir <path>]");
        Console.WriteLine();
        Console.WriteLine("In 'auto' mode the relay watches mp_control.json (written by the game's");
        Console.WriteLine("Settings ▸ Multiplayer tab) and hosts or joins to match. 'host'/'join' are");
        Console.WriteLine("the manual modes and ignore the in-game selection.");
        Console.WriteLine();
        Console.WriteLine("--instance <id> targets the game's Instance folder (id 'p2' => momi_mp_p2),");
        Console.WriteLine("for running a 2nd game + relay on one machine. --dir overrides it entirely.");
        Console.WriteLine();
        Console.WriteLine($"Default port : {DefaultPort}");
        Console.WriteLine($"Auto-detected dir : {ResolveMpDir()}");
    }

    static int Err(string msg) { Console.Error.WriteLine($"Error: {msg}"); PrintUsage(); return 1; }


    readonly record struct Control(string Mode, string Ip, int Port, long Seq);

    static async Task<Control?> ReadControlAsync(string path, CancellationToken ct)
    {
        var raw = await ReadTextSharedAsync(path, ct);
        if (raw is null) return null;
        try
        {
            var obj = JsonNode.Parse(raw)?.AsObject();
            if (obj is null) return null;
            var mode = obj["mode"]?.GetValue<string>() ?? "off";
            var ip   = obj["ip"]?.GetValue<string>()   ?? "127.0.0.1";
            return new Control(mode, ip, ReadInt(obj["port"]), ReadLong(obj["seq"]));
        }
        catch { return null; }
    }

    static int ReadInt(JsonNode? n)
    {
        if (n is null) return 0;
        try { return n.GetValue<int>(); }
        catch { try { return (int)n.GetValue<double>(); } catch { return 0; } }
    }

    static long ReadLong(JsonNode? n)
    {
        if (n is null) return 0;
        try { return n.GetValue<long>(); }
        catch { try { return (long)n.GetValue<double>(); } catch { return 0; } }
    }


    static async Task<int> RunAutoAsync(
        int defaultPort, string mpDir, string outPath, string remotePath,
        StatusReporter reporter, CancellationToken ct)
    {
        var controlPath = Path.Combine(mpDir, "mp_control.json");
        long lastSeq    = -1;

        CancellationTokenSource? sessionCts  = null;
        Task?                    sessionTask = null;

        reporter.Set("idle", "off", 0);
        Console.WriteLine("[AUTO] Watching mp_control.json — set Host/Join in the game's");
        Console.WriteLine("[AUTO] Settings ▸ Multiplayer tab. Ctrl+C to quit.");

        async Task TearDownAsync()
        {
            if (sessionCts is null) return;
            try { await sessionCts.CancelAsync(); } catch { }
            try { if (sessionTask is not null) await sessionTask; } catch { }
            sessionCts.Dispose();
            sessionCts  = null;
            sessionTask = null;
            try { if (File.Exists(remotePath)) File.Delete(remotePath); } catch { }
        }

        while (!ct.IsCancellationRequested)
        {
            var ctrl = await ReadControlAsync(controlPath, ct);
            if (ctrl is { } c && c.Seq != lastSeq)
            {
                lastSeq = c.Seq;
                await TearDownAsync();

                int usePort = c.Port > 0 ? c.Port : defaultPort;
                Console.WriteLine($"[AUTO] control: mode={c.Mode} ip={c.Ip} port={usePort} (seq {c.Seq})");

                if (c.Mode is "host" or "join")
                {
                    sessionCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    var stok   = sessionCts.Token;
                    if (c.Mode == "host")
                    {
                        reporter.Set("listening", "host", 0);
                        sessionTask = Task.Run(() =>
                            RunHostAsync(usePort, mpDir, outPath, remotePath, reporter, stok), stok);
                    }
                    else
                    {
                        reporter.Set("connecting", "join", 0);
                        sessionTask = Task.Run(() =>
                            RunClientAsync(c.Ip, usePort, mpDir, outPath, remotePath, reporter, stok), stok);
                    }
                }
                else
                {
                    reporter.Set("idle", "off", 0);
                }
            }

            try { await Task.Delay(PollMs, ct); }
            catch (OperationCanceledException) { break; }
        }

        await TearDownAsync();
        return 0;
    }


    static async Task SendAsync(NetworkStream stream, string json, CancellationToken ct)
    {
        var body   = Encoding.UTF8.GetBytes(json);
        var header = BitConverter.GetBytes(body.Length);
        await stream.WriteAsync(header, ct);
        await stream.WriteAsync(body,   ct);
    }

    static async Task SendLockedAsync(ClientSession session, string json, CancellationToken ct)
    {
        await session.WriteLock.WaitAsync(ct);
        try { await SendAsync(session.Stream, json, ct); }
        finally { session.WriteLock.Release(); }
    }

    static async Task<string?> ReceiveAsync(NetworkStream stream, CancellationToken ct)
    {
        var hdr = new byte[4];
        if (!await FillAsync(stream, hdr, ct)) return null;
        int len = BitConverter.ToInt32(hdr);
        if (len is <= 0 or > 4 * 1024 * 1024) return null;
        var body = new byte[len];
        if (!await FillAsync(stream, body, ct)) return null;
        return Encoding.UTF8.GetString(body);
    }

    static async Task<bool> FillAsync(NetworkStream stream, byte[] buf, CancellationToken ct)
    {
        int pos = 0;
        while (pos < buf.Length)
        {
            int n = await stream.ReadAsync(buf.AsMemory(pos), ct);
            if (n == 0) return false;
            pos += n;
        }
        return true;
    }

    static string BuildRemoteJson(ConcurrentDictionary<string, JsonObject> states, string? excludePid)
    {
        var arr = new JsonArray();
        foreach (var (pid, state) in states)
            if (pid != excludePid)
                arr.Add(state.DeepClone());
        return new JsonObject { ["players"] = arr }.ToJsonString();
    }

    static JsonObject? TryParseState(string json)
    {
        try   { return JsonNode.Parse(json)?.AsObject(); }
        catch { return null; }
    }

    static async Task<string?> ReadTextSharedAsync(string path, CancellationToken ct)
    {
        for (int attempt = 0; attempt < 6; attempt++)
        {
            try
            {
                using var fs = new FileStream(
                    path, FileMode.Open, FileAccess.Read,
                    FileShare.ReadWrite | FileShare.Delete);
                using var sr = new StreamReader(fs, Encoding.UTF8);
                return await sr.ReadToEndAsync(ct);
            }
            catch (FileNotFoundException)      { return null; }
            catch (DirectoryNotFoundException) { return null; }
            catch (IOException)                { await Task.Delay(15, ct); } // locked — retry
        }
        return null;
    }

    static async Task<byte[]?> ReadBytesSharedAsync(string path, CancellationToken ct)
    {
        for (int attempt = 0; attempt < 6; attempt++)
        {
            try
            {
                using var fs = new FileStream(
                    path, FileMode.Open, FileAccess.Read,
                    FileShare.ReadWrite | FileShare.Delete);
                using var ms = new MemoryStream();
                await fs.CopyToAsync(ms, ct);
                return ms.ToArray();
            }
            catch (FileNotFoundException)      { return null; }
            catch (DirectoryNotFoundException) { return null; }
            catch (IOException)                { await Task.Delay(15, ct); }
        }
        return null;
    }

    static async Task WriteRemoteAsync(
        string remotePath, string json, SemaphoreSlim writeLock, CancellationToken ct)
    {
        var tmp = remotePath + ".tmp";
        await writeLock.WaitAsync(ct);
        try
        {
            await File.WriteAllTextAsync(tmp, json, ct);
            for (int attempt = 0; ; attempt++)
            {
                try { File.Move(tmp, remotePath, overwrite: true); return; }
                catch (IOException) when (attempt < 5) { await Task.Delay(15, ct); }
            }
        }
        finally { writeLock.Release(); }
    }


    static async Task<int> RunHostAsync(
        int port, string mpDir, string outPath, string remotePath,
        StatusReporter reporter, CancellationToken ct)
    {
        var states       = new ConcurrentDictionary<string, JsonObject>();
        var sessions     = new ConcurrentDictionary<ClientSession, byte>();
        var writeLock    = new SemaphoreSlim(1, 1);
        var snapshotLock = new SemaphoreSlim(1, 1);   // one world serialize at a time
        string? myPid    = null;

        void RefreshPeers() => reporter.Set("listening", "host", sessions.Count);

        void PushToAll()
        {
            foreach (var (s, _) in sessions)
                if (s.PlayerId is { } pid)
                    s.Push(BuildRemoteJson(states, pid));
        }

        var listener = new TcpListener(IPAddress.Any, port);
        listener.Start();
        reporter.Set("listening", "host", 0);
        Console.WriteLine($"[HOST] Listening on :{port}");
        Console.WriteLine($"[HOST] Friends: set Join + your IP in their Multiplayer tab (port {port})");

        try
        {
            // Accept clients in background
            _ = Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    TcpClient client;
                    try   { client = await listener.AcceptTcpClientAsync(ct); }
                    catch { break; }
                    client.NoDelay = true;
                    var ep      = (IPEndPoint)client.Client.RemoteEndPoint!;
                    var session = new ClientSession(client);
                    sessions.TryAdd(session, 0);
                    RefreshPeers();
                    Console.WriteLine($"[HOST] + {ep.Address}");
                    _ = Task.Run(() => HostClientReadLoop(session, ep.Address.ToString(),
                        states, sessions, mpDir, remotePath, writeLock, snapshotLock,
                        () => myPid, PushToAll, RefreshPeers, ct), ct);
                    _ = Task.Run(() => ClientWriteLoop(session, ct), ct);
                }
            }, ct);

            // Poll own out.json
            string? lastRaw = null;
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var raw = await ReadTextSharedAsync(outPath, ct);
                    if (raw != null && raw != lastRaw)
                    {
                        lastRaw = raw;
                        var state = TryParseState(raw);
                        if (state != null)
                        {
                            var pid = state["player_id"]?.GetValue<string>();
                            if (pid != null)
                            {
                                myPid       = pid;
                                states[pid] = state;
                                await WriteRemoteAsync(remotePath, BuildRemoteJson(states, pid), writeLock, ct);
                                PushToAll();
                            }
                        }
                    }
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)               { Console.WriteLine($"[HOST] {ex.Message}"); }

                await Task.Delay(PollMs, ct);
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            listener.Stop();
            foreach (var (s, _) in sessions) { try { s.Tcp.Dispose(); } catch { } }
            Console.WriteLine("[HOST] Stopped listening.");
        }
        return 0;
    }

    // Reads state from one connected client, updates the shared table, then
    // immediately pushes the new snapshot to all other clients. Also handles
    // control messages (mp_msg) such as world-snapshot requests.
    static async Task HostClientReadLoop(
        ClientSession session, string addr,
        ConcurrentDictionary<string, JsonObject> states,
        ConcurrentDictionary<ClientSession, byte> sessions,
        string mpDir,
        string hostRemotePath,
        SemaphoreSlim writeLock,
        SemaphoreSlim snapshotLock,
        Func<string?> getHostPid,
        Action pushToAll,
        Action refreshPeers,
        CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var msg = await ReceiveAsync(session.Stream, ct);
                if (msg is null) break;

                var obj = TryParseState(msg);
                if (obj is null) continue;

                // Control channel (world snapshot request, etc.)
                if (obj["mp_msg"]?.GetValue<string>() is { } kind)
                {
                    if (kind == "snap_req")
                    {
                        Console.WriteLine($"[HOST] {addr} requested world snapshot");
                        _ = Task.Run(() => HandleSnapshotRequestAsync(session, mpDir, snapshotLock, ct), ct);
                    }
                    continue;
                }

                var pid = obj["player_id"]?.GetValue<string>();
                if (pid is null) continue;

                if (session.PlayerId != pid)
                {
                    session.PlayerId = pid;
                    Console.WriteLine($"[HOST] {addr} → '{pid}'");
                }

                states[pid] = obj;

                // Refresh host's own remote.json (sees this client's new state)
                var hostPid = getHostPid();
                if (hostPid != null)
                    await WriteRemoteAsync(hostRemotePath, BuildRemoteJson(states, hostPid), writeLock, ct);

                // Push updated snapshot to every connected client
                pushToAll();
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) { Console.WriteLine($"[HOST] {addr}: {ex.Message}"); }
        finally
        {
            if (session.PlayerId is { } pid)
            {
                states.TryRemove(pid, out _);
                Console.WriteLine($"[HOST] - {addr} ({pid})");
            }
            sessions.TryRemove(session, out _);
            refreshPeers();
            session.Outbox.Writer.TryComplete();
            session.Tcp.Dispose();
        }
    }

    // Produces a fresh world snapshot on the host game, then streams it to one
    // requesting client. Only one runs at a time (snapshotLock) so concurrent
    // joins don't race on the snapshot files.
    static async Task HandleSnapshotRequestAsync(
        ClientSession session, string mpDir, SemaphoreSlim snapshotLock, CancellationToken ct)
    {
        byte[]? jsonBytes;
        byte[]? binBytes;

        await snapshotLock.WaitAsync(ct);
        try
        {
            var reqPath   = Path.Combine(mpDir, "mp_snap_request");
            var readyPath = Path.Combine(mpDir, "mp_snap_ready");

            try { if (File.Exists(readyPath)) File.Delete(readyPath); } catch { }

            // Ask the host game to serialize the world.
            await File.WriteAllTextAsync(reqPath, "{}", ct);

            // Wait for the game to signal completion.
            var start = DateTime.UtcNow;
            while (!File.Exists(readyPath))
            {
                if ((DateTime.UtcNow - start).TotalMilliseconds > SnapshotTimeoutMs)
                {
                    Console.WriteLine("[HOST] snapshot timed out (is the host in-world?)");
                    return;
                }
                await Task.Delay(100, ct);
            }
            try { File.Delete(readyPath); } catch { }

            jsonBytes = await ReadBytesSharedAsync(Path.Combine(mpDir, "world_snapshot.json"), ct);
            binBytes  = await ReadBytesSharedAsync(Path.Combine(mpDir, "world_farm_terrain.bin"), ct);
        }
        finally { snapshotLock.Release(); }

        if (jsonBytes is null)
        {
            Console.WriteLine("[HOST] snapshot produced no world_snapshot.json");
            return;
        }

        try
        {
            await SendFileChunksAsync(session, "world_snapshot.json", jsonBytes, ct);
            if (binBytes is not null)
                await SendFileChunksAsync(session, "world_farm_terrain.bin", binBytes, ct);
            await SendLockedAsync(session, new JsonObject { ["mp_msg"] = "snap_done" }.ToJsonString(), ct);
            Console.WriteLine($"[HOST] snapshot sent to {session.PlayerId ?? "client"} " +
                              $"({jsonBytes.Length} + {(binBytes?.Length ?? 0)} bytes)");
        }
        catch (Exception ex) { Console.WriteLine($"[HOST] snapshot send failed: {ex.Message}"); }
    }

    // Streams one file as base64 chunks: snap_begin, N x snap_chunk, snap_end.
    static async Task SendFileChunksAsync(
        ClientSession session, string name, byte[] bytes, CancellationToken ct)
    {
        var b64   = Convert.ToBase64String(bytes);
        int total = (b64.Length + ChunkChars - 1) / ChunkChars;

        await SendLockedAsync(session, new JsonObject
        {
            ["mp_msg"] = "snap_begin",
            ["name"]   = name,
            ["chunks"] = total,
            ["bytes"]  = bytes.Length,
        }.ToJsonString(), ct);

        for (int i = 0; i < total; i++)
        {
            int off = i * ChunkChars;
            var part = b64.Substring(off, Math.Min(ChunkChars, b64.Length - off));
            await SendLockedAsync(session, new JsonObject
            {
                ["mp_msg"] = "snap_chunk",
                ["name"]   = name,
                ["seq"]    = i,
                ["data"]   = part,
            }.ToJsonString(), ct);
        }

        await SendLockedAsync(session, new JsonObject
        {
            ["mp_msg"] = "snap_end",
            ["name"]   = name,
        }.ToJsonString(), ct);
    }

    // Drains the per-session outbox and sends messages to the client. Uses the
    // shared write lock so state frames never interleave with snapshot frames.
    static async Task ClientWriteLoop(ClientSession session, CancellationToken ct)
    {
        try
        {
            await foreach (var msg in session.Outbox.Reader.ReadAllAsync(ct))
                await SendLockedAsync(session, msg, ct);
        }
        catch { }
    }

    // Send and receive run concurrently on the same TCP stream.
    // Sending is fire-and-forget every 100 ms; receiving is immediate on push.
    // On first connect the client asks the host for its world snapshot.

    static async Task<int> RunClientAsync(
        string host, int port, string mpDir, string outPath, string remotePath,
        StatusReporter reporter, CancellationToken ct)
    {
        bool requestedSnapshot = false;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                using var tcp = new TcpClient { NoDelay = true };
                reporter.Set("connecting", "join", 0);
                Console.WriteLine($"[CLIENT] Connecting to {host}:{port}…");
                await tcp.ConnectAsync(host, port, ct);
                reporter.Set("connected", "join", 1);
                Console.WriteLine("[CLIENT] Connected!");
                var stream = tcp.GetStream();

                using var linkCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

                // Ask for the host's world exactly once per relay session, so a
                // network blip that reconnects mid-session won't wipe local play.
                if (!requestedSnapshot)
                {
                    await SendAsync(stream, new JsonObject { ["mp_msg"] = "snap_req" }.ToJsonString(), linkCts.Token);
                    requestedSnapshot = true;
                    Console.WriteLine("[CLIENT] Requested world snapshot from host.");
                }

                var send    = ClientSendLoop(stream, mpDir, outPath, linkCts.Token);
                var receive = ClientReceiveLoop(stream, mpDir, remotePath, linkCts.Token);

                // Whichever loop dies first (network error, disconnect) ends both
                await Task.WhenAny(send, receive);
                await linkCts.CancelAsync();
                try { await Task.WhenAll(send, receive); } catch { }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)               { Console.WriteLine($"[CLIENT] {ex.Message}"); }

            if (!ct.IsCancellationRequested)
            {
                reporter.Set("connecting", "join", 0);
                try { File.Delete(remotePath); } catch { }
                Console.WriteLine("[CLIENT] Reconnecting in 3 s…");
                try { await Task.Delay(3000, ct); }
                catch (OperationCanceledException) { break; }
            }
        }
        return 0;
    }

    static async Task ClientSendLoop(NetworkStream stream, string mpDir, string outPath, CancellationToken ct)
    {
        var resyncPath = Path.Combine(mpDir, "mp_resync");
        while (!ct.IsCancellationRequested)
        {
            try
            {
                // If Game dropped a resync trigger (new day) then re-request the world.
                if (File.Exists(resyncPath))
                {
                    try { File.Delete(resyncPath); } catch { }
                    await SendAsync(stream, new JsonObject { ["mp_msg"] = "snap_req" }.ToJsonString(), ct);
                    Console.WriteLine("[CLIENT] New day — re-requesting world snapshot.");
                }

                var raw = await ReadTextSharedAsync(outPath, ct);
                if (raw != null)
                    await SendAsync(stream, raw, ct);
            }
            catch (OperationCanceledException) { return; }
            catch (Exception ex)               { Console.WriteLine($"[CLIENT] Send: {ex.Message}"); return; }

            await Task.Delay(PollMs, ct);
        }
    }

    static async Task ClientReceiveLoop(NetworkStream stream, string mpDir, string remotePath, CancellationToken ct)
    {
        var snap = new SnapshotReceiver(mpDir);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var msg = await ReceiveAsync(stream, ct);
                if (msg is null) return;

                // Cheap gate to only parse when it's actually a control message,
                // so the remote.json path stays a plain file write.
                if (msg.Contains("\"mp_msg\""))
                {
                    var obj = TryParseState(msg);
                    if (obj?["mp_msg"]?.GetValue<string>() is { } kind)
                    {
                        await snap.HandleAsync(kind, obj, ct);
                        continue;
                    }
                }

                await File.WriteAllTextAsync(remotePath, msg, ct);
            }
            catch (OperationCanceledException) { return; }
            catch (Exception ex)               { Console.WriteLine($"[CLIENT] Receive: {ex.Message}"); return; }
        }
    }
}
