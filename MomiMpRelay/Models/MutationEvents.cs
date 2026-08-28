using System.Text.Json;
using System.Text.Json.Serialization;
using MomiMpRelay.Configuration;

namespace MomiMpRelay.Models;

public static class MutationJson
{
    public static JsonSerializerOptions Options { get; } = new()
    {
        AllowTrailingCommas = false,
        ReadCommentHandling = JsonCommentHandling.Disallow,
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
        WriteIndented = false,
    };

    public static MutationEnvelope DeserializeAndValidate(string json)
    {
        var envelope = JsonSerializer.Deserialize<MutationEnvelope>(json, Options)
            ?? throw new JsonException("Mutation envelope cannot be null.");
        MutationValidator.EnsureValid(envelope);
        return envelope;
    }
}

public sealed record MutationEnvelope(
    [property: JsonPropertyName("protocol")] int Protocol,
    [property: JsonPropertyName("sessionId")] string SessionId,
    [property: JsonPropertyName("playerId")] string PlayerId,
    [property: JsonPropertyName("clientEpoch")] string ClientEpoch,
    [property: JsonPropertyName("clientSeq")] long ClientSeq,
    [property: JsonPropertyName("eventId")] string EventId,
    [property: JsonPropertyName("event")] MutationEvent Event,
    // Unassigned (outbox/pre-ledger) envelopes omit relaySeq; the relay assigns it starting at 1.
    [property: JsonPropertyName("relaySeq"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)] long RelaySeq = 0);

[JsonPolymorphic(TypeDiscriminatorPropertyName = "k")]
[JsonDerivedType(typeof(SpawnMutation), "spawn")]
[JsonDerivedType(typeof(GoneMutation), "gone")]
[JsonDerivedType(typeof(HitMutationEvent), "hit")]
[JsonDerivedType(typeof(FurnitureSpawnMutation), "fspawn")]
[JsonDerivedType(typeof(BuildingSpawnMutation), "bspawn")]
[JsonDerivedType(typeof(ContainerInventoryMutation), "cinv")]
[JsonDerivedType(typeof(CropStateMutation), "cstate")]
[JsonDerivedType(typeof(TerrainGroundKindMutation), "tgk")]
[JsonDerivedType(typeof(TerrainWateredMutation), "tw")]
[JsonDerivedType(typeof(ItemSpawnMutation), "isp")]
[JsonDerivedType(typeof(ItemPickupMutation), "ipk")]
[JsonDerivedType(typeof(AnimalStateMutation), "astate")]
[JsonDerivedType(typeof(BellMutation), "bell")]
public abstract record MutationEvent(
    [property: JsonPropertyName("s"), JsonRequired] int Sequence,
    [property: JsonPropertyName("loc"), JsonRequired] int LocationId);

public sealed record SpawnMutation(
    int Sequence,
    int LocationId,
    [property: JsonPropertyName("tx"), JsonRequired] int TileX,
    [property: JsonPropertyName("ty"), JsonRequired] int TileY,
    [property: JsonPropertyName("oid"), JsonRequired] int ObjectId,
    [property: JsonPropertyName("hp")] int? HitPoints = null) : MutationEvent(Sequence, LocationId);

public sealed record GoneMutation(
    int Sequence,
    int LocationId,
    [property: JsonPropertyName("cx"), JsonRequired] int CellX,
    [property: JsonPropertyName("cy"), JsonRequired] int CellY,
    [property: JsonPropertyName("oid"), JsonRequired] int ObjectId) : MutationEvent(Sequence, LocationId);

public sealed record HitMutationEvent(
    int Sequence,
    int LocationId,
    [property: JsonPropertyName("cx"), JsonRequired] int CellX,
    [property: JsonPropertyName("cy"), JsonRequired] int CellY,
    [property: JsonPropertyName("oid"), JsonRequired] int ObjectId,
    [property: JsonPropertyName("ehp"), JsonRequired] int ExpectedHitPoints,
    [property: JsonPropertyName("rhp"), JsonRequired] int ResultingHitPoints) : MutationEvent(Sequence, LocationId);

public sealed record FurnitureSpawnMutation(
    int Sequence,
    int LocationId,
    [property: JsonPropertyName("obj"), JsonRequired] JsonElement Object,
    [property: JsonPropertyName("invs"), JsonRequired] JsonElement[] Inventories) : MutationEvent(Sequence, LocationId);

public sealed record BuildingSpawnMutation(
    int Sequence,
    int LocationId,
    [property: JsonPropertyName("obj"), JsonRequired] JsonElement Object,
    [property: JsonPropertyName("invs"), JsonRequired] JsonElement[] Inventories,
    [property: JsonPropertyName("dyn")] JsonElement? DynamicGrid = null) : MutationEvent(Sequence, LocationId);

public sealed record ContainerInventoryMutation(
    int Sequence,
    int LocationId,
    [property: JsonPropertyName("tx"), JsonRequired] int TileX,
    [property: JsonPropertyName("ty"), JsonRequired] int TileY,
    [property: JsonPropertyName("oid"), JsonRequired] int ObjectId,
    [property: JsonPropertyName("inv"), JsonRequired] JsonElement Inventory,
    [property: JsonPropertyName("esig"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? ExpectedSignature = null) : MutationEvent(Sequence, LocationId);

public sealed record CropStateMutation(
    int Sequence,
    int LocationId,
    [property: JsonPropertyName("tx"), JsonRequired] int TileX,
    [property: JsonPropertyName("ty"), JsonRequired] int TileY,
    [property: JsonPropertyName("oid"), JsonRequired] int ObjectId,
    [property: JsonPropertyName("st"), JsonRequired] int Stage,
    [property: JsonPropertyName("dc"), JsonRequired] int DayCount,
    [property: JsonPropertyName("rc"), JsonRequired] int RegrowCycle,
    [property: JsonPropertyName("mt"), JsonRequired] int ManagedTimer,
    [property: JsonPropertyName("cf"), JsonRequired] int Flags,
    [property: JsonPropertyName("esig"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? ExpectedSignature = null) : MutationEvent(Sequence, LocationId);

public sealed record TerrainGroundKindMutation(
    int Sequence,
    int LocationId,
    [property: JsonPropertyName("cx"), JsonRequired] int CellX,
    [property: JsonPropertyName("cy"), JsonRequired] int CellY,
    [property: JsonPropertyName("gk"), JsonRequired] int GroundKind) : MutationEvent(Sequence, LocationId);

public sealed record TerrainWateredMutation(
    int Sequence,
    int LocationId,
    [property: JsonPropertyName("cx"), JsonRequired] int CellX,
    [property: JsonPropertyName("cy"), JsonRequired] int CellY,
    [property: JsonPropertyName("w"), JsonRequired] bool Watered) : MutationEvent(Sequence, LocationId);

public sealed record ItemSpawnMutation(
    int Sequence,
    int LocationId,
    [property: JsonPropertyName("g"), JsonRequired] string ItemGid,
    [property: JsonPropertyName("x"), JsonRequired] double WorldX,
    [property: JsonPropertyName("y"), JsonRequired] double WorldY,
    [property: JsonPropertyName("its"), JsonRequired] JsonElement[] Items) : MutationEvent(Sequence, LocationId);

public sealed record ItemPickupMutation(
    int Sequence,
    int LocationId,
    [property: JsonPropertyName("g"), JsonRequired] string ItemGid) : MutationEvent(Sequence, LocationId);

public sealed record AnimalStateMutation(
    int Sequence,
    int LocationId,
    [property: JsonPropertyName("btlx"), JsonRequired] int BuildingTileX,
    [property: JsonPropertyName("btly"), JsonRequired] int BuildingTileY,
    [property: JsonPropertyName("oid"), JsonRequired] int ObjectId,
    [property: JsonPropertyName("idx"), JsonRequired] int AnimalIndex,
    [property: JsonPropertyName("pat"), JsonRequired] bool Patted,
    [property: JsonPropertyName("eat"), JsonRequired] bool Eaten,
    [property: JsonPropertyName("out"), JsonRequired] bool Outside,
    [property: JsonPropertyName("hpts"), JsonRequired] int HeartPoints,
    [property: JsonPropertyName("prod")] int? ProductionDays = null,
    [property: JsonPropertyName("esig"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? ExpectedSignature = null) : MutationEvent(Sequence, LocationId);

public sealed record BellMutation(
    int Sequence,
    int LocationId,
    [property: JsonPropertyName("btlx"), JsonRequired] int BuildingTileX,
    [property: JsonPropertyName("btly"), JsonRequired] int BuildingTileY,
    [property: JsonPropertyName("oid"), JsonRequired] int ObjectId,
    [property: JsonPropertyName("out"), JsonRequired] bool Outside) : MutationEvent(Sequence, LocationId);

public static class MutationEventKind
{
    public static string GetKind(MutationEvent mutation) => mutation switch
    {
        SpawnMutation => "spawn",
        GoneMutation => "gone",
        HitMutationEvent => "hit",
        FurnitureSpawnMutation => "fspawn",
        BuildingSpawnMutation => "bspawn",
        ContainerInventoryMutation => "cinv",
        CropStateMutation => "cstate",
        TerrainGroundKindMutation => "tgk",
        TerrainWateredMutation => "tw",
        ItemSpawnMutation => "isp",
        ItemPickupMutation => "ipk",
        AnimalStateMutation => "astate",
        BellMutation => "bell",
        _ => throw new ArgumentOutOfRangeException(nameof(mutation), "Unknown mutation event kind."),
    };
}

public static class MutationValidator
{
    public static IReadOnlyList<string> Validate(MutationEvent mutation)
    {
        ArgumentNullException.ThrowIfNull(mutation);
        var errors = new List<string>();
        ValidateEvent(mutation, errors);
        return errors;
    }

    public static void EnsureValid(MutationEvent mutation)
    {
        var errors = Validate(mutation);
        if (errors.Count > 0)
            throw new JsonException(string.Join(" ", errors));
    }

    public static IReadOnlyList<string> Validate(MutationEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        var errors = new List<string>();
        if (envelope.Protocol != RelaySession.ProtocolVersion) errors.Add($"protocol must be {RelaySession.ProtocolVersion}.");
        if (string.IsNullOrWhiteSpace(envelope.SessionId)) errors.Add("sessionId is required.");
        if (string.IsNullOrWhiteSpace(envelope.PlayerId)) errors.Add("playerId is required.");
        if (string.IsNullOrWhiteSpace(envelope.ClientEpoch)) errors.Add("clientEpoch is required.");
        if (envelope.ClientSeq < 0) errors.Add("clientSeq cannot be negative.");
        if (string.IsNullOrWhiteSpace(envelope.EventId)) errors.Add("eventId is required.");
        if (envelope.RelaySeq < 0) errors.Add("relaySeq cannot be negative.");
        if (envelope.Event is null) errors.Add("event is required.");
        else ValidateEvent(envelope.Event, errors);
        return errors;
    }

    public static void EnsureValid(MutationEnvelope envelope)
    {
        var errors = Validate(envelope);
        if (errors.Count > 0)
            throw new JsonException(string.Join(" ", errors));
    }

    static void ValidateEvent(MutationEvent mutation, List<string> errors)
    {
        if (mutation.Sequence < 0) errors.Add("event.s cannot be negative.");
        if (mutation.LocationId < 0) errors.Add("event.loc cannot be negative.");

        switch (mutation)
        {
            case SpawnMutation value:
                ValidateTile(value.TileX, value.TileY, errors);
                break;
            case GoneMutation value:
                ValidateCell(value.CellX, value.CellY, errors);
                break;
            case HitMutationEvent value:
                ValidateCell(value.CellX, value.CellY, errors);
                if (value.ExpectedHitPoints < 0) errors.Add("event.ehp cannot be negative.");
                if (value.ResultingHitPoints < 0) errors.Add("event.rhp cannot be negative.");
                if (value.ResultingHitPoints >= value.ExpectedHitPoints)
                    errors.Add("event.rhp must be less than event.ehp.");
                break;
            case ContainerInventoryMutation value:
                ValidateTile(value.TileX, value.TileY, errors);
                ValidateOpaque(value.Inventory, "event.inv", errors);
                ValidateOptionalSignature(value.ExpectedSignature, "event.esig", errors);
                break;
            case CropStateMutation value:
                ValidateTile(value.TileX, value.TileY, errors);
                ValidateOptionalSignature(value.ExpectedSignature, "event.esig", errors);
                break;
            case TerrainGroundKindMutation value:
                ValidateCell(value.CellX, value.CellY, errors);
                break;
            case TerrainWateredMutation value:
                ValidateCell(value.CellX, value.CellY, errors);
                break;
            case ItemSpawnMutation value:
                ValidateGid(value.ItemGid, errors);
                ValidateFinite(value.WorldX, "event.x", errors);
                ValidateFinite(value.WorldY, "event.y", errors);
                ValidateOpaqueArray(value.Items, "event.its", errors);
                break;
            case ItemPickupMutation value:
                ValidateGid(value.ItemGid, errors);
                break;
            case AnimalStateMutation value:
                ValidateCell(value.BuildingTileX, value.BuildingTileY, errors);
                if (value.AnimalIndex < 0) errors.Add("event.idx cannot be negative.");
                if (value.HeartPoints < 0) errors.Add("event.hpts cannot be negative.");
                ValidateOptionalSignature(value.ExpectedSignature, "event.esig", errors);
                break;
            case BellMutation value:
                ValidateCell(value.BuildingTileX, value.BuildingTileY, errors);
                break;
            case FurnitureSpawnMutation value:
                ValidateOpaque(value.Object, "event.obj", errors);
                ValidateOpaqueArray(value.Inventories, "event.invs", errors);
                break;
            case BuildingSpawnMutation value:
                ValidateOpaque(value.Object, "event.obj", errors);
                ValidateOpaqueArray(value.Inventories, "event.invs", errors);
                if (value.DynamicGrid is { } dynamicGrid && dynamicGrid.ValueKind is not (JsonValueKind.Object or JsonValueKind.Array or JsonValueKind.Null))
                    errors.Add("event.dyn must be a JSON object, array, or null.");
                break;
        }
    }

    static void ValidateTile(int x, int y, List<string> errors) => ValidateCoordinates(x, y, "tile", errors);
    static void ValidateCell(int x, int y, List<string> errors) => ValidateCoordinates(x, y, "cell", errors);

    static void ValidateCoordinates(int x, int y, string name, List<string> errors)
    {
        if (x < 0 || y < 0) errors.Add($"event {name} coordinates cannot be negative.");
    }

    static void ValidateGid(string value, List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(value)) errors.Add("event.g must not be blank.");
    }

    static void ValidateOptionalSignature(string? value, string name, List<string> errors)
    {
        if (value is not null && string.IsNullOrWhiteSpace(value)) errors.Add($"{name} must not be blank when present.");
    }

    static void ValidateFinite(double value, string name, List<string> errors)
    {
        if (!double.IsFinite(value)) errors.Add($"{name} must be finite.");
    }

    static void ValidateOpaque(JsonElement value, string name, List<string> errors)
    {
        if (value.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null)
            errors.Add($"{name} must contain JSON.");
    }

    static void ValidateOpaqueArray(JsonElement[]? values, string name, List<string> errors)
    {
        if (values is null)
        {
            errors.Add($"{name} is required.");
            return;
        }
        for (var index = 0; index < values.Length; index++)
            ValidateOpaque(values[index], $"{name}[{index}]", errors);
    }
}