using System.Collections.Concurrent;
using System.Text.Json;
using Dapper;
using Microsoft.Data.Sqlite;
using MomiMpRelay.Configuration;
using MomiMpRelay.Models;

namespace MomiMpRelay.Ledger;

public sealed record MutationAcceptResult(long RelaySeq, bool IsDuplicate);

public sealed record RepairRequestRecord(string PlayerId, string Reason, long ReportedCursor, string RequestedAtUtc);

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
            reported_cursor  INTEGER NOT NULL DEFAULT 0,
            requested_at_utc TEXT    NOT NULL
        );
        """;
    private ConcurrentDictionary<string, long> _lastPublishedRelaySeqs = new ConcurrentDictionary<string, long>();
    readonly SqliteConnection _connection;
    readonly Lock _sync = new();

    public MutationLedger(string mpDir)
    {
        var databasePath = Path.Combine(mpDir, "mp_mutation_ledger.sqlite3");
        Directory.CreateDirectory(mpDir);
        _connection = new SqliteConnection($"Data Source={databasePath}");
        _connection.Open();
        _connection.Execute("PRAGMA journal_mode=WAL;");
        _connection.Execute("PRAGMA synchronous=NORMAL;"); // safe with WAL; avoids an fsync per commit
        _connection.Execute(Schema);
    }

    /// <summary>Transactionally dedupes by eventId and allocates the next global relaySeq for a new event.</summary>
    public MutationAcceptResult Accept(MutationEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        lock (_sync)
        {
            using var transaction = _connection.BeginTransaction();
            var result = AcceptCore(envelope, transaction);
            transaction.Commit();
            return result;
        }
    }

    /// <summary>Accepts many envelopes in one transaction; a malformed entry is skipped without losing the rest of the batch.</summary>
    public IReadOnlyList<MutationAcceptResult?> AcceptMany(IReadOnlyList<MutationEnvelope> envelopes)
    {
        ArgumentNullException.ThrowIfNull(envelopes);
        var results = new MutationAcceptResult?[envelopes.Count];
        lock (_sync)
        {
            using var transaction = _connection.BeginTransaction();
            for (var i = 0; i < envelopes.Count; i++)
            {
                try
                {
                    results[i] = AcceptCore(envelopes[i], transaction);
                }
                catch (Exception)
                {
                    results[i] = null; // malformed/invalid; skip without aborting the rest of the batch
                }
            }
            transaction.Commit();
        }
        return results;
    }

    MutationAcceptResult AcceptCore(MutationEnvelope envelope, SqliteTransaction transaction)
    {
        MutationValidator.EnsureValid(envelope);

        EnsureSession(envelope, transaction);

        var existingRelaySeq = _connection.QuerySingleOrDefault<long?>(
            """
            SELECT relay_seq FROM producer_events
            WHERE session_id = @SessionId AND event_id = @EventId
            """,
            new { envelope.SessionId, envelope.EventId }, transaction);

        if (existingRelaySeq is { } duplicateRelaySeq)
            return new MutationAcceptResult(duplicateRelaySeq, IsDuplicate: true);

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

        _lastPublishedRelaySeqs[envelope.SessionId] = relaySeq;
        return new MutationAcceptResult(relaySeq, IsDuplicate: false);
    }

    public long GetHeadRelaySeq(string sessionId)
    {
        if (_lastPublishedRelaySeqs.TryGetValue(sessionId, out var lastPublished))
            return lastPublished;
        lock (_sync)
        {
            lastPublished = _connection.QuerySingleOrDefault<long?>(
                "SELECT head_relay_seq FROM sessions WHERE session_id = @sessionId",
                new { sessionId }) ?? 0;
            _lastPublishedRelaySeqs[sessionId] = lastPublished;
            return lastPublished;
        }
    }

    /// <summary>Reconstructs canonical envelopes strictly after the given relaySeq, ascending, for distribution.</summary>
    public IReadOnlyList<MutationEnvelope> GetEventsAfter(string sessionId, long afterRelaySeq, int maxCount)
    {
        lock (_sync)
        {
            var rows = _connection.Query<ProducerEventRow>(
                """
                SELECT relay_seq AS RelaySeq, event_id AS EventId, player_id AS PlayerId,
                       client_epoch AS ClientEpoch, client_seq AS ClientSeq, payload_json AS PayloadJson
                FROM producer_events
                WHERE session_id = @sessionId AND relay_seq > @afterRelaySeq
                ORDER BY relay_seq
                LIMIT @maxCount
                """,
                new { sessionId, afterRelaySeq, maxCount });

            var events = new List<MutationEnvelope>();
            foreach (var row in rows)
            {
                var mutationEvent = JsonSerializer.Deserialize<MutationEvent>(row.PayloadJson, MutationJson.Options)
                    ?? throw new InvalidOperationException($"Ledger event {row.EventId} could not be deserialized.");
                events.Add(new MutationEnvelope(RelaySession.ProtocolVersion, sessionId, row.PlayerId,
                    row.ClientEpoch, row.ClientSeq, row.EventId, mutationEvent, row.RelaySeq));
            }
            return events;
        }
    }

    public long GetClientCursor(string sessionId, string playerId)
    {
        lock (_sync)
        {
            return GetClientCursorCore(sessionId, playerId);
        }
    }

    public void PruneSessionProducerEvents(string sessionId)
    {
        lock (_sync)
        {
            long lowestCursor = GetLowestClientCursor(sessionId);

            _connection.Execute(
                "DELETE FROM producer_events WHERE session_id = @sessionId AND relay_seq <= @lowestCursor",
                new { sessionId, lowestCursor });
        }
    }

    long GetClientCursorCore(string sessionId, string playerId) =>
        _connection.QuerySingleOrDefault<long?>(
            "SELECT last_applied_relay_seq FROM client_progress WHERE session_id = @sessionId AND player_id = @playerId",
            new { sessionId, playerId }) ?? 0;

    long GetLowestClientCursor(string sessionId) =>
        _connection.QuerySingleOrDefault<long?>(
            "SELECT MIN(last_applied_relay_seq) FROM client_progress WHERE session_id = @sessionId",
            new { sessionId }) ?? 0;

    /// <summary>Advances a client's durable applied cursor; never moves it backward.</summary>
    public void RecordClientCursor(string sessionId, string playerId, long relaySeq)
    {
        lock (_sync)
        {
            using var transaction = _connection.BeginTransaction();
            var current = GetClientCursorCore(sessionId, playerId);
            if (relaySeq <= current)
            {
                transaction.Commit();
                return;
            }

            _connection.Execute(
                """
                INSERT INTO client_progress (session_id, player_id, last_applied_relay_seq, updated_at_utc)
                VALUES (@sessionId, @playerId, @relaySeq, @updatedAtUtc)
                ON CONFLICT (session_id, player_id) DO UPDATE SET
                    last_applied_relay_seq = excluded.last_applied_relay_seq,
                    updated_at_utc = excluded.updated_at_utc
                """,
                new { sessionId, playerId, relaySeq, updatedAtUtc = DateTime.UtcNow.ToString("o") }, transaction);

            transaction.Commit();
        }
    }

    /// <summary>Durably logs a client-reported inbox repair request for diagnostics.</summary>
    public void RecordRepairRequest(string sessionId, string playerId, string reason, long reportedCursor)
    {
        lock (_sync)
        {
            _connection.Execute(
                """
                INSERT INTO repair_requests (session_id, player_id, reason, reported_cursor, requested_at_utc)
                VALUES (@sessionId, @playerId, @reason, @reportedCursor, @requestedAtUtc)
                """,
                new { sessionId, playerId, reason, reportedCursor, requestedAtUtc = DateTime.UtcNow.ToString("o") });
        }
    }

    public IReadOnlyList<RepairRequestRecord> GetRepairRequests(string sessionId, int maxCount = 100)
    {
        lock (_sync)
        {
            return _connection.Query<RepairRequestRecord>(
                """
                SELECT player_id AS PlayerId, reason AS Reason, reported_cursor AS ReportedCursor, requested_at_utc AS RequestedAtUtc
                FROM repair_requests
                WHERE session_id = @sessionId
                ORDER BY id DESC
                LIMIT @maxCount
                """,
                new { sessionId, maxCount }).ToList();
        }
    }

    sealed record ProducerEventRow(long RelaySeq, string EventId, string PlayerId, string ClientEpoch, long ClientSeq, string PayloadJson);

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
