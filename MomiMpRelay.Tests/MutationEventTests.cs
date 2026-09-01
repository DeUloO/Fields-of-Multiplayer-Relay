using System.Text.Json;
using System.Text.Json.Nodes;
using MomiMpRelay.Models;

namespace MomiMpRelay.Tests;

public sealed class MutationEventTests
{
    [Theory]
    [InlineData("{\"k\":\"spawn\",\"s\":42,\"loc\":1,\"tx\":10,\"ty\":12,\"oid\":405,\"hp\":3}", typeof(SpawnMutation))]
    [InlineData("{\"k\":\"gone\",\"s\":43,\"loc\":1,\"cx\":10,\"cy\":12,\"oid\":405}", typeof(GoneMutation))]
    [InlineData("{\"k\":\"hit\",\"s\":44,\"loc\":1,\"cx\":10,\"cy\":12,\"oid\":405,\"ehp\":3,\"rhp\":2}", typeof(HitMutationEvent))]
    [InlineData("{\"k\":\"fspawn\",\"s\":45,\"loc\":1,\"obj\":{\"opaque\":true},\"invs\":[[{\"item\":true}]]}", typeof(FurnitureSpawnMutation))]
    [InlineData("{\"k\":\"bspawn\",\"s\":46,\"loc\":1,\"obj\":{\"opaque\":true},\"invs\":[[{\"item\":true}]],\"dyn\":{\"opaque\":true}}", typeof(BuildingSpawnMutation))]
    [InlineData("{\"k\":\"cinv\",\"s\":47,\"loc\":1,\"tx\":10,\"ty\":12,\"oid\":405,\"inv\":[{\"item\":true}]}", typeof(ContainerInventoryMutation))]
    [InlineData("{\"k\":\"cstate\",\"s\":48,\"loc\":1,\"tx\":10,\"ty\":12,\"oid\":405,\"st\":3,\"dc\":4,\"rc\":0,\"mt\":-1,\"cf\":0}", typeof(CropStateMutation))]
    [InlineData("{\"k\":\"tgk\",\"s\":49,\"loc\":1,\"cx\":10,\"cy\":12,\"gk\":2}", typeof(TerrainGroundKindMutation))]
    [InlineData("{\"k\":\"tw\",\"s\":50,\"loc\":1,\"cx\":10,\"cy\":12,\"w\":true}", typeof(TerrainWateredMutation))]
    [InlineData("{\"k\":\"isp\",\"s\":51,\"loc\":1,\"g\":\"1:384:224:2\",\"x\":384.0,\"y\":224.0,\"its\":[{\"opaque\":true}]}", typeof(ItemSpawnMutation))]
    [InlineData("{\"k\":\"ipk\",\"s\":52,\"loc\":1,\"g\":\"1:384:224:2\"}", typeof(ItemPickupMutation))]
    [InlineData("{\"k\":\"astate\",\"s\":53,\"loc\":0,\"btlx\":20,\"btly\":30,\"oid\":900,\"idx\":0,\"pat\":true,\"eat\":false,\"out\":true,\"hpts\":8,\"prod\":2}", typeof(AnimalStateMutation))]
    [InlineData("{\"k\":\"bell\",\"s\":54,\"loc\":0,\"btlx\":20,\"btly\":30,\"oid\":900,\"out\":true}", typeof(BellMutation))]
    public void EveryCatalogEventDeserializesAndRoundTripsCompactNames(string json, Type expectedType)
    {
        var mutation = JsonSerializer.Deserialize<MutationEvent>(json, MutationJson.Options);

        Assert.NotNull(mutation);
        Assert.IsType(expectedType, mutation);

        using var document = JsonDocument.Parse(JsonSerializer.Serialize(mutation, MutationJson.Options));
        using var expected = JsonDocument.Parse(json);
        AssertJsonEquivalent(expected.RootElement, document.RootElement);
        Assert.DoesNotContain("Sequence", document.RootElement.ToString());
        Assert.DoesNotContain("LocationId", document.RootElement.ToString());
    }

    [Fact]
    public void EnvelopePreservesRelayMetadataSeparatelyFromEvent()
    {
        const string json = "{\"protocol\":2,\"sessionId\":\"session-1\",\"playerId\":\"player-1\",\"clientEpoch\":\"epoch-1\",\"clientSeq\":7,\"eventId\":\"event-7\",\"relaySeq\":12,\"event\":{\"k\":\"ipk\",\"s\":52,\"loc\":1,\"g\":\"1:384:224:2\"}}";

        var envelope = MutationJson.DeserializeAndValidate(json);

        Assert.Equal(2, envelope.Protocol);
        Assert.Equal("session-1", envelope.SessionId);
        Assert.Equal("player-1", envelope.PlayerId);
        Assert.Equal("epoch-1", envelope.ClientEpoch);
        Assert.Equal(7, envelope.ClientSeq);
        Assert.Equal("event-7", envelope.EventId);
        Assert.Equal(12, envelope.RelaySeq);
        Assert.IsType<ItemPickupMutation>(envelope.Event);
        using var expected = JsonDocument.Parse(json);
        using var actual = JsonDocument.Parse(JsonSerializer.Serialize(envelope, MutationJson.Options));
        AssertJsonEquivalent(expected.RootElement, actual.RootElement);
    }

    [Fact]
    public void EnvelopeRejectsUnsupportedProtocolVersion()
    {
        var eventValue = JsonSerializer.Deserialize<MutationEvent>(
            "{\"k\":\"ipk\",\"s\":1,\"loc\":1,\"g\":\"drop\"}", MutationJson.Options)!;
        var envelope = new MutationEnvelope(1, "session", "player", "epoch", 1, "event", eventValue, 0);

        Assert.NotEmpty(MutationValidator.Validate(envelope));
    }

    [Fact]
    public void UnknownEventKindIsRejected()
    {
        Assert.Throws<JsonException>(() =>
            JsonSerializer.Deserialize<MutationEvent>("{\"k\":\"turn_in_box\",\"s\":1,\"loc\":1}", MutationJson.Options));
    }

    [Fact]
    public void HitRequiresExpandedHitpointFields()
    {
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<MutationEvent>(
            "{\"k\":\"hit\",\"s\":44,\"loc\":1,\"cx\":10,\"cy\":12,\"oid\":405,\"ehp\":3}",
            MutationJson.Options));
    }

    [Fact]
    public void HitRejectsLegacyDamageField()
    {
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<MutationEvent>(
            "{\"k\":\"hit\",\"s\":44,\"loc\":1,\"cx\":10,\"cy\":12,\"oid\":405,\"ehp\":3,\"rhp\":2,\"dmg\":1}",
            MutationJson.Options));
    }

    [Theory]
    [InlineData("{\"k\":\"cinv\",\"s\":47,\"loc\":1,\"tx\":10,\"ty\":12,\"oid\":405,\"inv\":[{\"item\":true}],\"esig\":\"prev-sig\"}")]
    [InlineData("{\"k\":\"cstate\",\"s\":48,\"loc\":1,\"tx\":10,\"ty\":12,\"oid\":405,\"st\":3,\"dc\":4,\"rc\":0,\"mt\":-1,\"cf\":0,\"esig\":\"prev-sig\"}")]
    [InlineData("{\"k\":\"astate\",\"s\":53,\"loc\":0,\"btlx\":20,\"btly\":30,\"oid\":900,\"idx\":0,\"pat\":true,\"eat\":false,\"out\":true,\"hpts\":8,\"prod\":2,\"esig\":\"prev-sig\"}")]
    public void OptionalExpectedSignatureRoundTripsWhenPresent(string json)
    {
        var mutation = JsonSerializer.Deserialize<MutationEvent>(json, MutationJson.Options)!;

        using var document = JsonDocument.Parse(JsonSerializer.Serialize(mutation, MutationJson.Options));
        using var expected = JsonDocument.Parse(json);
        AssertJsonEquivalent(expected.RootElement, document.RootElement);
    }

    [Fact]
    public void OptionalExpectedSignatureRejectsBlankValue()
    {
        var eventValue = JsonSerializer.Deserialize<MutationEvent>(
            "{\"k\":\"cinv\",\"s\":47,\"loc\":1,\"tx\":10,\"ty\":12,\"oid\":405,\"inv\":[{\"item\":true}],\"esig\":\" \"}",
            MutationJson.Options)!;

        Assert.NotEmpty(MutationValidator.Validate(eventValue));
    }

    [Theory]
    [InlineData("{\"k\":\"hit\",\"s\":1,\"loc\":1,\"cx\":0,\"cy\":0,\"oid\":1,\"ehp\":-1,\"rhp\":0}")]
    [InlineData("{\"k\":\"hit\",\"s\":1,\"loc\":1,\"cx\":0,\"cy\":0,\"oid\":1,\"ehp\":2,\"rhp\":2}")]
    [InlineData("{\"k\":\"hit\",\"s\":1,\"loc\":1,\"cx\":0,\"cy\":0,\"oid\":1,\"ehp\":3,\"rhp\":-1}")]
    [InlineData("{\"k\":\"isp\",\"s\":1,\"loc\":1,\"g\":\" \" ,\"x\":0,\"y\":0,\"its\":[]}")]
    [InlineData("{\"k\":\"spawn\",\"s\":1,\"loc\":1,\"tx\":-1,\"ty\":0,\"oid\":1}")]
    public void ValidationRejectsObviousInvalidValues(string eventJson)
    {
        var eventValue = JsonSerializer.Deserialize<MutationEvent>(eventJson, MutationJson.Options)!;
        var envelope = new MutationEnvelope(2, "session", "player", "epoch", 1, "event", eventValue, 0);

        Assert.NotEmpty(MutationValidator.Validate(envelope));
    }

    [Fact]
    public void StrictOptionsRejectUnknownPayloadFields()
    {
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<MutationEvent>(
            "{\"k\":\"ipk\",\"s\":1,\"loc\":1,\"g\":\"drop\",\"extra\":true}", MutationJson.Options));
    }

    static void AssertJsonEquivalent(JsonElement expected, JsonElement actual)
    {
        Assert.Equal(expected.ValueKind, actual.ValueKind);
        switch (expected.ValueKind)
        {
            case JsonValueKind.Object:
                var expectedProperties = expected.EnumerateObject().ToArray();
                var actualProperties = actual.EnumerateObject().ToArray();
                Assert.Equal(expectedProperties.Length, actualProperties.Length);
                foreach (var property in expectedProperties)
                {
                    Assert.True(actual.TryGetProperty(property.Name, out var actualValue),
                        $"Missing property '{property.Name}'.");
                    AssertJsonEquivalent(property.Value, actualValue);
                }
                break;
            case JsonValueKind.Array:
                var expectedItems = expected.EnumerateArray().ToArray();
                var actualItems = actual.EnumerateArray().ToArray();
                Assert.Equal(expectedItems.Length, actualItems.Length);
                for (var index = 0; index < expectedItems.Length; index++)
                    AssertJsonEquivalent(expectedItems[index], actualItems[index]);
                break;
            case JsonValueKind.Number:
                Assert.Equal(expected.GetDouble(), actual.GetDouble());
                break;
            case JsonValueKind.String:
                Assert.Equal(expected.GetString(), actual.GetString());
                break;
            case JsonValueKind.True:
            case JsonValueKind.False:
            case JsonValueKind.Null:
                break;
        }
    }
}