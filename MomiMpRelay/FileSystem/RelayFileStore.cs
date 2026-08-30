using System.Text;
using MomiMpRelay.Logging;

namespace MomiMpRelay.FileSystem;

public static class RelayFileStore
{
    public static async Task<byte[]> GetSha256Async(string path, CancellationToken ct)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete);
        using var sha256 = System.Security.Cryptography.SHA256.Create();
        var hash = await sha256.ComputeHashAsync(fs, ct);
        return hash;
    }
    public static async Task<string?> ReadTextSharedAsync(string path, CancellationToken ct)
    {
        for (int attempt = 0; attempt < 6; attempt++)
        {
            try
            {
                using var fs = new FileStream(path, FileMode.Open, FileAccess.Read,
                    FileShare.ReadWrite | FileShare.Delete);
                using var sr = new StreamReader(fs, Encoding.UTF8);
                return await sr.ReadToEndAsync(ct);
            }
            catch (FileNotFoundException) { return null; }
            catch (DirectoryNotFoundException) { return null; }
            catch (IOException) { await Task.Delay(15, ct); }
            catch (UnauthorizedAccessException) { await Task.Delay(15, ct); }
        }
        return null;
    }

    public static async Task<byte[]?> ReadBytesSharedAsync(string path, CancellationToken ct)
    {
        for (int attempt = 0; attempt < 6; attempt++)
        {
            try
            {
                using var fs = new FileStream(path, FileMode.Open, FileAccess.Read,
                    FileShare.ReadWrite | FileShare.Delete);
                using var ms = new MemoryStream();
                await fs.CopyToAsync(ms, ct);
                return ms.ToArray();
            }
            catch (FileNotFoundException) { return null; }
            catch (DirectoryNotFoundException) { return null; }
            catch (IOException) { await Task.Delay(15, ct); }
            catch (UnauthorizedAccessException) { await Task.Delay(15, ct); }
        }
        return null;
    }

    public static async Task<bool> WriteRemoteAsync(string remotePath, string json,
        SemaphoreSlim writeLock, CancellationToken ct)
    {
        var tmp = remotePath + ".tmp";
        await writeLock.WaitAsync(ct);
        try
        {
            await File.WriteAllTextAsync(tmp, json, ct);
            for (int attempt = 0; attempt < 6; attempt++)
            {
                try
                {
                    File.Move(tmp, remotePath, overwrite: true);
                    return true;
                }
                catch (IOException) when (attempt < 5) { await Task.Delay(15, ct); }
                catch (UnauthorizedAccessException) when (attempt < 5) { await Task.Delay(15, ct); }
            }
            try
            {
                await File.WriteAllTextAsync(remotePath, json, ct);
                return true;
            }
            catch (Exception ex)
            {
                RelayLogger.Error($"[RELAY] write remote.json fallback failed: {ex.Message}");
                return false;
            }
        }
        catch (Exception ex)
        {
            RelayLogger.Error($"[RELAY] write remote.json failed: {ex.Message}");
            return false;
        }
        finally { writeLock.Release(); }
    }

    public static void CleanupTemporaryFiles(string directory)
    {
        try
        {
            foreach (var path in Directory.EnumerateFiles(directory, "*.tmp"))
                try
                {
                    File.Delete(path);
                }
                catch { }
            foreach (var path in Directory.EnumerateFiles(directory, "*.part"))
                try
                {
                    File.Delete(path);
                }
                catch { }
        }
        catch (Exception ex) { RelayLogger.Error($"[RELAY] temporary-file cleanup failed: {ex.Message}"); }
    }
}
