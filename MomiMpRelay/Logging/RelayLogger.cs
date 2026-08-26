namespace MomiMpRelay.Logging;

public static class RelayLogger
{
    static readonly object Gate = new();

    public static void Info(string message) => Write(Console.Out, message);
    public static void Error(string message) => Write(Console.Error, message);

    static void Write(TextWriter writer, string message)
    {
        lock (Gate)
        {
            writer.WriteLine($"[{DateTime.Now:HH:mm:ss}] {message}");
        }
    }
}
