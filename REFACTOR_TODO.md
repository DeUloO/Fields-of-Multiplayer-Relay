# MomiMpRelay Refactor TODO

This is the persistent backlog for the remaining netcode and design work. The tracked application source is `MomiMpRelay/Program.cs`; there is no duplicate root-level `Program.cs` to resolve.

## Highest Priority

- [x] Add a host/client loopback integration test.
  - Start a host and client using loopback networking.
  - Verify the LiteNetLib handshake and connection acceptance.
  - Exchange player state in both directions.
  - Exercise disconnect and reconnect behavior.

- [x] Add an end-to-end snapshot transfer test.
  - Transfer both the JSON world snapshot and binary terrain file.
  - Verify raw bytes at the destination.
  - Test missing, duplicate, out-of-order, truncated, and oversized chunks.
  - Test cancellation during transfer.

- [x] Make snapshot completion transfer-aware.
  - Track completion for the complete snapshot operation, not only individual files.
  - Create `mp_apply_world` only after every required file has completed successfully.
  - Clean up open streams and `.part` files on cancellation, disconnect, or invalid input.

- [x] Strengthen `RelayPacketCodec` validation.
  - Validate the minimum binary snapshot header length in the codec.
  - Decode file ID and sequence into a typed packet model.
  - Reject malformed packet payloads before they reach application code.
  - Add tests for truncated headers and invalid snapshot metadata.

- [x] Replace string-based JSON dispatch.
  - Remove `msg.Contains("\"mp_msg\"")` from the client receive path.
  - Parse each JSON packet once into a typed envelope or validated message.
  - Dispatch control messages, player state, and remote-state updates by type.

## Architecture

- [x] Extract the host workflow from `Program.cs` into `Modes/RelayHost.cs`.
  - Own host peer/session management.
  - Own player-state aggregation and broadcasting.
  - Own host-side snapshot coordination.

- [x] Extract the client workflow into `Modes/RelayClient.cs`.
  - Own connection and reconnect lifecycle.
  - Own network polling task.
  - Own local-state sending and remote-state receiving.

- [x] Extract auto-mode orchestration into `Modes/AutoRelay.cs`.
  - Own `mp_control.json` watching.
  - Own mode transitions and session teardown.

- [x] Introduce dependency boundaries for mode classes.
  - Inject file storage, transport, status reporting, and time/retry behavior.
  - Reduce static helper calls and closure-heavy orchestration.

## Reliability and Error Handling

- [x] Make `RelayFileStore.WriteRemoteAsync` report final write failure.
  - Return a success/failure result or throw after the fallback fails.
  - Do not report a failed write as successful.

- [x] Add connection timeout handling.
  - Bound the wait for the initial client connection.
  - Surface a useful status detail before reconnecting.

- [x] Preserve network failure details.
  - Log `OnNetworkError` information.
  - Log relevant `DisconnectInfo` details.
  - Distinguish timeout, rejection, remote shutdown, and socket failure where possible.

- [x] Define channel backpressure policies explicitly.
  - Keep lossy behavior only for replaceable state snapshots.
  - Prevent snapshot/control messages from being dropped.
  - Bound inbound traffic or separate state traffic from transfer traffic.
  - Add tests for overflow behavior.

- [x] Add cleanup for interrupted filesystem operations.
  - Remove stale `.tmp` and `.part` files at startup where appropriate.
  - Ensure cancellation and exceptions dispose snapshot streams.

- [x] Improve reconnect policy.
  - Add bounded exponential backoff and jitter.
  - Avoid retrying too aggressively when the host is unavailable.

## Test Coverage

- [ ] Test `SnapshotReceiver` cancellation and cleanup paths.
- [ ] Test `StatusReporter` cancellation behavior.
- [ ] Test transient file-lock retry behavior in `RelayFileStore`.
- [x] Test invalid and partial control files.
- [ ] Test packet sequence boundaries and large transfer metadata.
- [ ] Test multiple clients updating state concurrently.
- [ ] Test host shutdown while clients are connected.
- [ ] Test client shutdown during an active snapshot.
- [ ] Add stress coverage for rapid state updates and slow consumers.

## Lower Priority

- [ ] Dispose owned `SemaphoreSlim`, `NetManager`, and task resources consistently.
- [ ] Centralize or synchronize console logging if interleaved output becomes a problem.
- [ ] Add protocol version or capability negotiation once message evolution requires it.
- [ ] Replace magic snapshot file IDs with a typed file identifier enum shared by sender and receiver.
- [ ] Consider reducing snapshot packet overhead or measuring transfer performance with realistic world sizes.
