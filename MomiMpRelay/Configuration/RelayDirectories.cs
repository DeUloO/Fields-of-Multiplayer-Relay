namespace MomiMpRelay.Configuration;

public static class RelayDirectories
{
    public static string FomRoot => Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "FieldsOfMistria");

    public static string InstanceDir(string id)
    {
        var clean = new string(id.Where(c => char.IsLetterOrDigit(c) || c is '_' or '-').ToArray());
        if (clean.Length == 0 || clean.Equals("main", StringComparison.OrdinalIgnoreCase))
            return Path.Combine(FomRoot, "momi_mp");
        return Path.Combine(FomRoot, "momi_mp_" + clean);
    }

    public static string ResolveMpDir()
    {
        var direct = Path.Combine(FomRoot, "momi_mp");
        var candidates = new List<string> { direct };
        try
        {
            foreach (var sub in Directory.GetDirectories(FomRoot))
            {
                var mm = Path.Combine(sub, "momi_mp");
                if (Directory.Exists(mm))
                    candidates.Add(mm);
            }
        }
        catch { }

        string? best = null;
        DateTime bestTime = DateTime.MinValue;
        foreach (var candidate in candidates)
        {
            try
            {
                var control = Path.Combine(candidate, "mp_control.json");
                if (File.Exists(control))
                {
                    var time = File.GetLastWriteTimeUtc(control);
                    if (time > bestTime)
                    {
                        bestTime = time;
                        best = candidate;
                    }
                }
            }
            catch { }
        }
        return best ?? direct;
    }
}
