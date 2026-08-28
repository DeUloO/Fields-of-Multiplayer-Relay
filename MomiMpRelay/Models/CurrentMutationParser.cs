using System.Text.Json;
using System.Text.Json.Nodes;

namespace MomiMpRelay.Models;

public static class CurrentMutationParser
{
    public static IReadOnlyList<MutationEvent> ParseEvents(JsonObject playerState)
    {
        ArgumentNullException.ThrowIfNull(playerState);

        if (playerState["evs"] is null)
            return Array.Empty<MutationEvent>();

        if (playerState["evs"] is not JsonArray events)
            throw new JsonException("Player state evs must be an array.");

        var mutations = new List<MutationEvent>(events.Count);
        foreach (var node in events)
        {
            if (node is null)
                throw new JsonException("Player state evs cannot contain null events.");

            var mutation = node.Deserialize<MutationEvent>(MutationJson.Options)
                ?? throw new JsonException("Player state event cannot be null.");
            MutationValidator.EnsureValid(mutation);
            mutations.Add(mutation);
        }

        return mutations;
    }
}