using System.Text;
using MomiMpRelay.Models;
using System.Text.Json.Nodes;

namespace MomiMpRelay.FileSystem;

static class RelayFileStore
{
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

    public static async Task WriteRemoteAsync(string remotePath, string json,
        SemaphoreSlim writeLock, CancellationToken ct)
    {
        var tmp = remotePath + ".tmp";
        await writeLock.WaitAsync(ct);
        try
        {
            await File.WriteAllTextAsync(tmp, json, ct);
            for (int attempt = 0; attempt < 6; attempt++)
            {
                try { File.Move(tmp, remotePath, overwrite: true); return; }
                catch (IOException) when (attempt < 5) { await Task.Delay(15, ct); }
                catch (UnauthorizedAccessException) when (attempt < 5) { await Task.Delay(15, ct); }
            }
            try { await File.WriteAllTextAsync(remotePath, json, ct); } catch { }
        }
        catch (Exception ex) { Console.WriteLine($"[RELAY] write remote.json: {ex.Message}"); }
        finally { writeLock.Release(); }
    }
}
