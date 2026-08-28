using System.Text.Json;
using Dapper;
using Microsoft.Data.Sqlite;
using MomiMpRelay.Configuration;
using MomiMpRelay.Models;

namespace MomiMpRelay.Ledger;

public sealed record MutationAcceptResult(long RelaySeq, bool IsDuplicate);

/// <summary>Durable, transactionally-sequenced record of accepted mutation events for one relay session.</summary>
public sealed class MutationLedger : IDisposable
{
    const string Schema = """
        CREATE TABLE IF NOT EXISTS sessions (
            session_id      TEXT    NOT NULL PRIMARY KEY,
            protocol        INTEGER NOT NULL,
            created_at_utc  TEXT    NOT NULL,
            head_relay_seq  INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS producer_events (
            session_id      TEXT    NOT NULL,
            relay_seq       INTEGER NOT NULL,
            event_id        TEXT    NOT NULL,
            player_id       TEXT    NOT NULL,
            client_epoch    TEXT    NOT NULL,
            client_seq      INTEGER NOT NULL,
            kind            TEXT    NOT NULL,
            location_id     INTEGER NOT NULL,
            payload_json    TEXT    NOT NULL,
            accepted_at_utc TEXT    NOT NULL,
            PRIMARY KEY (session_id, relay_seq),
            UNIQUE (session_id, event_id)
        );

        CREATE TABLE IF NOT EXISTS client_progress (
            session_id              TEXT    NOT NULL,
            player_id               TEXT    NOT NULL,
            last_applied_relay_seq  INTEGER NOT NULL DEFAULT 0,
            updated_at_utc          TEXT    NOT NULL,
            PRIMARY KEY (session_id, player_id)
        );

        CREATE TABLE IF NOT EXISTS checkpoints (
            session_id            TEXT    NOT NULL,
            checkpoint_id         TEXT    NOT NULL,
            checkpoint_relay_seq  INTEGER NOT NULL,
            snapshot_hash         TEXT,
            terrain_hash          TEXT,
            created_at_utc        TEXT    NOT NULL,
            PRIMARY KEY (session_id, checkpoint_id)
        );

        CREATE TABLE IF NOT EXISTS repair_requests (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id       TEXT    NOT NULL,
            player_id        TEXT    NOT NULL,
            reason           TEXT    NOT NULL,
            requested_at_utc TEXT    NOT NULL
        );
        """;

    readonly SqliteConnection _connection;
    readonly Lock _sync = new();

    public MutationLedger(string mpDir)
    {
        var databasePath = Path.Combine(mpDir, "mp_mutation_ledger.sqlite3");
        Directory.CreateDirectory(mpDir);
        _connection = new SqliteConnection($"Data Source={databasePath}");
        _connection.Open();
        _connection.Execute("PRAGMA journal_mode=WAL;");
        _connection.Execute(Schema);
    }

    /// <summary>Transactionally dedupes by eventId and allocates the next global relaySeq for a new event.</summary>
    public MutationAcceptResult Accept(MutationEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        MutationValidator.EnsureValid(envelope);

        lock (_sync)
        {
            using var transaction = _connection.BeginTransaction();

            EnsureSession(envelope, transaction);

            var existingRelaySeq = _connection.QuerySingleOrDefault<long?>(
                """
                SELECT relay_seq FROM producer_events
                WHERE session_id = @SessionId AND event_id = @EventId
                """,
                new { envelope.SessionId, envelope.EventId }, transaction);

            if (existingRelaySeq is { } duplicateRelaySeq)
            {
                transaction.Commit();
                return new MutationAcceptResult(duplicateRelaySeq, IsDuplicate: true);
            }

            var head = _connection.QuerySingle<long>(
                "SELECT head_relay_seq FROM sessions WHERE session_id = @SessionId",
                new { envelope.SessionId }, transaction);
            var relaySeq = head + 1;

            _connection.Execute(
                """
                INSERT INTO producer_events
                    (session_id, relay_seq, event_id, player_id, client_epoch, client_seq, kind, location_id, payload_json, accepted_at_utc)
                VALUES
                    (@SessionId, @RelaySeq, @EventId, @PlayerId, @ClientEpoch, @ClientSeq, @Kind, @LocationId, @PayloadJson, @AcceptedAtUtc)
                """,
                new
                {
                    envelope.SessionId,
                    RelaySeq = relaySeq,
                    envelope.EventId,
                    envelope.PlayerId,
                    envelope.ClientEpoch,
                    envelope.ClientSeq,
                    Kind = MutationEventKind.GetKind(envelope.Event),
                    LocationId = envelope.Event.LocationId,
                    PayloadJson = JsonSerializer.Serialize(envelope.Event, MutationJson.Options),
                    AcceptedAtUtc = DateTime.UtcNow.ToString("o"),
                }, transaction);

            _connection.Execute(
                "UPDATE sessions SET head_relay_seq = @RelaySeq WHERE session_id = @SessionId",
                new { envelope.SessionId, RelaySeq = relaySeq }, transaction);

            transaction.Commit();
            return new MutationAcceptResult(relaySeq, IsDuplicate: false);
        }
    }

    public long GetHeadRelaySeq(string sessionId) =>
        _connection.QuerySingleOrDefault<long?>(
            "SELECT head_relay_seq FROM sessions WHERE session_id = @sessionId",
            new { sessionId }) ?? 0;

    void EnsureSession(MutationEnvelope envelope, SqliteTransaction transaction)
    {
        _connection.Execute(
            """
            INSERT INTO sessions (session_id, protocol, created_at_utc, head_relay_seq)
            VALUES (@SessionId, @Protocol, @CreatedAtUtc, 0)
            ON CONFLICT (session_id) DO NOTHING
            """,
            new
            {
                envelope.SessionId,
                envelope.Protocol,
                CreatedAtUtc = DateTime.UtcNow.ToString("o"),
            }, transaction);
    }

    public void Dispose() => _connection.Dispose();
}
