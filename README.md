# Momi Multiplayer Relay

MomiMpRelay is a .NET 10 console relay for multiplayer state and world-snapshot data between Fields of Mistria game instances. It uses LiteNetLib for reliable ordered UDP transport and shared files for communication with the game.

## Requirements

- .NET SDK 10
- Windows, because the relay resolves the game's Local Application Data directory by default

## Build and Run

Build the solution:

```text
dotnet build MomiMpRelay.slnx
```

Run automatic mode, which follows the game's `mp_control.json` file:

```text
dotnet run --project MomiMpRelay/MomiMpRelay.csproj
```

Run a host explicitly:

```text
dotnet run --project MomiMpRelay/MomiMpRelay.csproj -- host --port 7777
```

Run a client explicitly:

```text
dotnet run --project MomiMpRelay/MomiMpRelay.csproj -- join 192.168.1.5 --port 7777
```

Use `--dir <path>` to override the relay directory or `--instance <id>` to target an instance-specific directory.

Help is available through `help`, `-h`, `--help`, or `/?`.

## Test Projects

The repository separates tests by speed and purpose.

### Unit Tests

Project: `MomiMpRelay.Tests`

These tests run quickly and do not start network processes. They cover:

- Relay message parsing and typed records
- Control-file deserialization, including fractional JSON ports such as `7777.0`
- Packet encoding and decoding
- Invalid and truncated packet rejection
- Snapshot receiver validation and cleanup
- Status-file generation and cancellation
- Shared file reads, writes, retries, and temporary-file cleanup
- Directory and instance-name resolution

Run them with:

```text
dotnet test MomiMpRelay.Tests/MomiMpRelay.Tests.csproj
```

The unit project uses xUnit and Moq.

### Integration Tests

Project: `MomiMpRelay.IntegrationTests`

These tests launch real relay processes and communicate over loopback UDP. They cover:

- LiteNetLib handshake and connection acceptance
- Bidirectional player-state exchange
- Client disconnect and reconnect behavior
- Complete raw-binary world and terrain snapshot transfer
- Multiple connected clients
- Host shutdown while a client is connected
- Client shutdown during snapshot preparation
- Rapid state updates converging to the final state

Run them separately with:

```text
dotnet test MomiMpRelay.IntegrationTests/MomiMpRelay.IntegrationTests.csproj
```

They are included in `MomiMpRelay.slnx` and therefore run with the normal solution test command.

### Stress Tests

Project: `MomiMpRelay.StressTests`

Stress tests launch multiple real relay clients and apply sustained rapid state updates, repeated reconnects, and higher client counts. They are deliberately excluded from `MomiMpRelay.slnx` so routine builds and test runs remain fast and deterministic.

Run the stress profile explicitly:

```text
dotnet test MomiMpRelay.StressTests/MomiMpRelay.StressTests.csproj --filter Category=Stress
```

The default profile runs five scenarios:

- Four clients with 500 updates per client
- Five client reconnect cycles
- Eight clients with 2,000 updates per client
- Four clients while repeatedly locking their `remote.json` files to simulate slow filesystem consumers
- A 30-second two-client soak run with continuous state updates

Configure the profiles with environment variables:

```powershell
$env:MOMI_STRESS_CLIENTS = "8"
$env:MOMI_STRESS_UPDATES = "2000"
$env:MOMI_STRESS_RECONNECTS = "10"
$env:MOMI_STRESS_HIGH_CLIENTS = "12"
$env:MOMI_STRESS_HIGH_UPDATES = "5000"
$env:MOMI_STRESS_SLOW_CLIENTS = "6"
$env:MOMI_STRESS_SLOW_UPDATES = "1000"
$env:MOMI_STRESS_SOAK_SECONDS = "60"
dotnet test MomiMpRelay.StressTests/MomiMpRelay.StressTests.csproj --filter Category=Stress
```

The stress project is intended for occasional manual or scheduled runs. It is excluded from `MomiMpRelay.slnx` so normal builds and test runs remain fast and deterministic.

## Project Structure

```text
MomiMpRelay.slnx
README.md
REFACTOR_TODO.md
MomiMpRelay/
  MomiMpRelay.csproj
  Program.cs
  Configuration/
    RelayDirectories.cs
  FileSystem/
    RelayFileStore.cs
  Logging/
    RelayLogger.cs
  Models/
    RelayMessages.cs
  Modes/
    AutoRelay.cs
    RelayClient.cs
    RelayHost.cs
  Networking/
    ClientSession.cs
    RelayListener.cs
    RelayPacketCodec.cs
    RelayTransport.cs
  Snapshots/
    SnapshotReceiver.cs
  Status/
    StatusReporter.cs
MomiMpRelay.Tests/
  MomiMpRelay.Tests.csproj
  *Tests.cs
MomiMpRelay.IntegrationTests/
  MomiMpRelay.IntegrationTests.csproj
  RelayIntegrationTests.cs
MomiMpRelay.StressTests/
  MomiMpRelay.StressTests.csproj
  RelayStressTests.cs
```

## Runtime Design

### Game-to-relay files

Each game instance has a relay directory containing files such as:

- `mp_control.json`: selects automatic host or join mode
- `out.json`: latest local player state
- `remote.json`: latest state for other players
- `mp_status.json`: relay heartbeat and connection status
- `mp_snap_request`: asks the host game to create a snapshot
- `mp_snap_ready`: signals that snapshot files are ready
- `world_snapshot.json`: JSON world snapshot
- `world_farm_terrain.bin`: binary terrain snapshot
- `mp_apply_world`: asks the client game to apply a completed snapshot

The default directory is under:

```text
%LOCALAPPDATA%\FieldsOfMistria\momi_mp
```

### Network protocol

LiteNetLib provides reliable ordered UDP delivery. Relay packets have a small binary discriminator:

- JSON packets contain typed relay messages encoded as UTF-8 JSON.
- Snapshot packets contain a typed file ID, sequence number, and raw binary bytes.

Snapshot data is sent in MTU-safe chunks. The receiver validates ordering, expected chunk counts, byte totals, and complete multi-file transfer state before creating `mp_apply_world`.

### Code ownership

- `Program.cs`: startup, argument parsing, directory selection, and mode selection
- `Modes`: host, client, and automatic-mode workflows
- `Networking`: LiteNetLib callbacks, sessions, transport, and packet codec
- `Models`: typed network and control-file messages
- `FileSystem`: shared-file access and atomic remote-state writes
- `Snapshots`: snapshot assembly, validation, and promotion
- `Status`: heartbeat status reporting
- `Logging`: synchronized process logging