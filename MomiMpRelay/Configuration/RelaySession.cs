namespace MomiMpRelay.Configuration;

public static class RelaySession
{
    public const int ProtocolVersion = 2;

    public static string SessionId { get; } = Guid.NewGuid().ToString("n");
}
