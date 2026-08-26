using System.Collections.Concurrent;
using System.Buffers.Binary;
using System.Net;
using LiteNetLib;
using LiteNetLib.Utils;
using MomiMpRelay.Configuration;
using MomiMpRelay.FileSystem;
using MomiMpRelay.Models;
using MomiMpRelay.Networking;
using MomiMpRelay.Snapshots;
using MomiMpRelay.Status;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Channels;

namespace MomiMpRelay;
static class Program
{
    const int DefaultPort = 7777;
    const int PollMs      = 50;    // poll out.json / control every 50 ms (~20 fps)

    // Raw bytes per snapshot packet, leaving room below a normal UDP MTU for
    // the packet kind, chunk metadata, JSON envelope, and LiteNetLib headers.
    const int ChunkBytes  = 900;

    // How long the host waits for its game to produce a snapshot (write
    // mp_snap_ready) after being asked. The game only serializes when in-world.
    const int SnapshotTimeoutMs = 20_000;

    static async Task<int> Main(string[] args)
    {
        bool   noArgs      = args.Length == 0;
        string firstArg    = noArgs ? "" : args[0].ToLowerInvariant();
        if (firstArg is "help" or "-h" or "--help" or "/?") { PrintUsage(); return 0; }

        bool   firstIsFlag = !noArgs && args[0].StartsWith('-');
        string mode        = (noArgs || firstIsFlag) ? "auto" : args[0].ToLowerInvariant();

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

        string mpDir =
            explicitDir
            ?? (instanceId != null ? RelayDirectories.InstanceDir(instanceId) : RelayDirectories.ResolveMpDir());

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
        Console.WriteLine($"Auto-detected dir : {RelayDirectories.ResolveMpDir()}");
    }

    static int Err(string msg) { Console.Error.WriteLine($"Error: {msg}"); PrintUsage(); return 1; }


    static async Task<RelayControl?> ReadControlAsync(string path, CancellationToken ct)
    {
        for (int attempt = 0; attempt < 6; attempt++)
        {
            try
            {
                await using var fs = new FileStream(
                    path, FileMode.Open, FileAccess.Read,
                    FileShare.ReadWrite | FileShare.Delete);
                return await JsonSerializer.DeserializeAsync<RelayControl>(fs, cancellationToken: ct);
            }
            catch (FileNotFoundException)      { return null; }
            catch (DirectoryNotFoundException) { return null; }
            catch (JsonException)              { return null; }
            catch (IOException)                { await Task.Delay(15, ct); }
            catch (UnauthorizedAccessException){ await Task.Delay(15, ct); }
        }
        return null;
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


    static string BuildRemoteJson(ConcurrentDictionary<string, JsonObject> states, string? excludePid)
    {
        var arr = new JsonArray();
        foreach (var (pid, state) in states)
            if (pid != excludePid)
                arr.Add(state.DeepClone());
        return new JsonObject { ["players"] = arr }.ToJsonString();
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
                s.Push(new RelayStateUpdate(JsonNode.Parse(BuildRemoteJson(states, pid))!.AsObject()));
        }

        RelayListener? netListener = null;
        var net = new NetManager(netListener = new RelayListener(
            (peer, addr) => //Connected
            {
                var session = new ClientSession(peer);
                sessions.TryAdd(session, 0);
                RefreshPeers();
                Console.WriteLine($"[HOST] + {addr}");
                _ = Task.Run(() => ClientWriteLoop(session, ct), ct);
                _ = Task.Run(() => HostClientReadLoop(session, states, sessions, mpDir,
                    remotePath, writeLock, snapshotLock, () => myPid, PushToAll,
                    RefreshPeers, ct), ct);
            },
            (peer, packet) => //Received
            {
                var session = sessions.Keys.FirstOrDefault(s => s.Peer == peer);
                if (session is not null)
                    session.Inbox.Writer.TryWrite(packet);
            },
            peer => //Disconnected
            {
                var session = sessions.Keys.FirstOrDefault(s => s.Peer == peer);
                if (session is null) return;
                session.Inbox.Writer.TryComplete();
                if (session.PlayerId is { } pid)
                {
                    states.TryRemove(pid, out _);
                    Console.WriteLine($"[HOST] - {peer.Address} ({pid})");
                }
                sessions.TryRemove(session, out _);
                session.Outbox.Writer.TryComplete();
                RefreshPeers();
            }));
        net.Start(port);
        using var pollCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var pollTask = Task.Run(async () =>
        {
            while (!pollCts.IsCancellationRequested)
            {
                net.PollEvents();
                try { await Task.Delay(10, pollCts.Token); }
                catch (OperationCanceledException) { break; }
            }
        }, pollCts.Token);
        reporter.Set("listening", "host", 0);
        Console.WriteLine($"[HOST] Listening on :{port}");
        Console.WriteLine($"[HOST] Friends: set Join + your IP in their Multiplayer tab (port {port})");

        try
        {
            string? lastRaw = null;
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var raw = await RelayFileStore.ReadTextSharedAsync(outPath, ct);
                    if (raw != null && raw != lastRaw)
                    {
                        lastRaw = raw;
                        var state = PlayerState.Parse(raw);
                        if (state != null)
                        {
                            myPid       = state.PlayerId;
                            states[state.PlayerId] = state.Payload;
                            await RelayFileStore.WriteRemoteAsync(remotePath, BuildRemoteJson(states, state.PlayerId), writeLock, ct);
                                PushToAll();
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
            await pollCts.CancelAsync();
            try { await pollTask; } catch (OperationCanceledException) { }
            net.Stop();
            foreach (var (s, _) in sessions) s.Outbox.Writer.TryComplete();
            Console.WriteLine("[HOST] Stopped listening.");
        }
        return 0;
    }

    // Reads state from one connected client, updates the shared table, then
    // immediately pushes the new snapshot to all other clients. Also handles
    // control messages (mp_msg) such as world-snapshot requests.
    static async Task HostClientReadLoop(
        ClientSession session,
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
            await foreach (var msg in session.Inbox.Reader.ReadAllAsync(ct))
            {
                if (msg.Kind != RelayPacketKind.Json) continue;
                await HostClientMessageAsync(session, session.Peer.Address.ToString(),
                    Encoding.UTF8.GetString(msg.Data), states, sessions, mpDir,
                    hostRemotePath, writeLock, snapshotLock, getHostPid, pushToAll,
                    refreshPeers, ct);
            }
        }
        catch (OperationCanceledException) { }
    }

    static async Task HostClientMessageAsync(
        ClientSession session, string addr,
        string msg,
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
            var control = RelayMessageParser.ParseControl(msg);
            if (control is not null)
            {
                if (control is SnapshotRequest)
                {
                    Console.WriteLine($"[HOST] {addr} requested world snapshot");
                    await HandleSnapshotRequestAsync(session, mpDir, snapshotLock, ct);
                }
                return;
            }

            var state = PlayerState.Parse(msg);
            if (state is null) return;

                if (session.PlayerId != state.PlayerId)
                {
                    session.PlayerId = state.PlayerId;
                    Console.WriteLine($"[HOST] {addr} → '{state.PlayerId}'");
                }

                states[state.PlayerId] = state.Payload;

                // Refresh host's own remote.json (sees this client's new state)
                var hostPid = getHostPid();
                if (hostPid != null)
                    await RelayFileStore.WriteRemoteAsync(hostRemotePath, BuildRemoteJson(states, hostPid), writeLock, ct);

                // Push updated snapshot to every connected client
                pushToAll();
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) { Console.WriteLine($"[HOST] {addr}: {ex.Message}"); }
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

            jsonBytes = await RelayFileStore.ReadBytesSharedAsync(Path.Combine(mpDir, "world_snapshot.json"), ct);
            binBytes  = await RelayFileStore.ReadBytesSharedAsync(Path.Combine(mpDir, "world_farm_terrain.bin"), ct);
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
            await RelayTransport.SendLockedAsync(session, new SnapshotDone(), ct);
            Console.WriteLine($"[HOST] snapshot sent to {session.PlayerId ?? "client"} " +
                              $"({jsonBytes.Length} + {(binBytes?.Length ?? 0)} bytes)");
        }
        catch (Exception ex) { Console.WriteLine($"[HOST] snapshot send failed: {ex.Message}"); }
    }

    // Streams one file as a begin envelope, raw binary chunks, and an end envelope.
    static async Task SendFileChunksAsync(
        ClientSession session, string name, byte[] bytes, CancellationToken ct)
    {
        byte fileId = name == "world_snapshot.json" ? (byte)1 : (byte)2;
        int total = (bytes.Length + ChunkBytes - 1) / ChunkBytes;

        await RelayTransport.SendLockedAsync(session, new SnapshotBegin(name, total, bytes.Length), ct);

        for (int i = 0; i < total; i++)
        {
            int off = i * ChunkBytes;
            int count = Math.Min(ChunkBytes, bytes.Length - off);
            await RelayTransport.SendLockedSnapshotChunkAsync(session, fileId, i, bytes, off, count, ct);
        }

        await RelayTransport.SendLockedAsync(session, new SnapshotEnd(name), ct);
    }

    // Drains the per-session outbox and sends messages to the client. Uses the
    // shared write lock so state frames never interleave with snapshot frames.
    static async Task ClientWriteLoop(ClientSession session, CancellationToken ct)
    {
        try
        {
            await foreach (var msg in session.Outbox.Reader.ReadAllAsync(ct))
                await RelayTransport.SendLockedAsync(session, msg, ct);
        }
        catch { }
    }

    // Sending is fire-and-forget every 100 ms; receiving is event-driven.
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
                var incoming = Channel.CreateUnbounded<RelayPacket>();
                var connected = new TaskCompletionSource<NetPeer>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                var listener = new RelayListener(
                    (peer, _) => connected.TrySetResult(peer),
                    (_, packet) => incoming.Writer.TryWrite(packet),
                    peer =>
                    {
                        incoming.Writer.TryComplete();
                        connected.TrySetException(new IOException(
                            $"Connection to {peer.Address} was closed."));
                    });
                var net = new NetManager(listener);
                using var pollCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                Task? poll = null;
                try
                {
                    net.Start();
                    reporter.Set("connecting", "join", 0);
                    Console.WriteLine($"[CLIENT] Connecting to {host}:{port}…");
                    net.Connect(host, port, RelayProtocol.ConnectionKey);
                    poll = Task.Run(async () =>
                    {
                        while (!pollCts.IsCancellationRequested)
                        {
                            net.PollEvents();
                            await Task.Delay(10, pollCts.Token);
                        }
                    }, pollCts.Token);
                    var peer = await connected.Task.WaitAsync(ct);
                    reporter.Set("connected", "join", 1);
                    Console.WriteLine("[CLIENT] Connected!");

                    using var linkCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

                // Ask for the host's world exactly once per relay session, so a
                // network blip that reconnects mid-session won't wipe local play.
                    if (!requestedSnapshot)
                    {
                        RelayTransport.Send(peer, new SnapshotRequest(), linkCts.Token);
                        requestedSnapshot = true;
                        Console.WriteLine("[CLIENT] Requested world snapshot from host.");
                    }

                    var send    = ClientSendLoop(peer, mpDir, outPath, linkCts.Token);
                    var receive = ClientReceiveLoop(incoming.Reader, mpDir, remotePath, linkCts.Token);

                // Whichever loop dies first (network error, disconnect) ends both
                    await Task.WhenAny(send, receive);
                    await linkCts.CancelAsync();
                    try { await Task.WhenAll(send, receive); } catch { }
                }
                finally
                {
                    await pollCts.CancelAsync();
                    net.Stop();
                    if (poll is not null)
                        try { await poll; } catch (OperationCanceledException) { }
                }
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

    static async Task ClientSendLoop(NetPeer peer, string mpDir, string outPath, CancellationToken ct)
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
                    RelayTransport.Send(peer, new SnapshotRequest(), ct);
                    Console.WriteLine("[CLIENT] New day — re-requesting world snapshot.");
                }

                var raw = await RelayFileStore.ReadTextSharedAsync(outPath, ct);
                var state = raw is null ? null : PlayerState.Parse(raw);
                if (state is not null)
                    RelayTransport.Send(peer, state, ct);
            }
            catch (OperationCanceledException) { return; }
            catch (Exception ex)               { Console.WriteLine($"[CLIENT] Send: {ex.Message}"); return; }

            await Task.Delay(PollMs, ct);
        }
    }

    static async Task ClientReceiveLoop(ChannelReader<RelayPacket> messages, string mpDir, string remotePath, CancellationToken ct)
    {
        var snap = new SnapshotReceiver(mpDir);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var packet = await messages.ReadAsync(ct);
                if (packet.Kind == RelayPacketKind.SnapshotChunk)
                {
                    if (packet.Data.Length >= 1 + sizeof(int))
                    {
                        var fileId = packet.Data[0];
                        var sequence = BinaryPrimitives.ReadInt32LittleEndian(
                            packet.Data.AsSpan(1));
                        await snap.HandleChunkAsync(fileId, sequence,
                            packet.Data[(1 + sizeof(int))..], ct);
                    }
                    continue;
                }
                if (packet.Kind != RelayPacketKind.Json) continue;
                var msg = Encoding.UTF8.GetString(packet.Data);

                // Cheap gate to only parse when it's actually a control message,
                // so the remote.json path stays a plain file write.
                if (msg.Contains("\"mp_msg\""))
                {
                    var control = RelayMessageParser.ParseControl(msg);
                    if (control is not null)
                    {
                        await snap.HandleAsync(control, ct);
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
