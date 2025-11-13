using System.Collections.Generic;

namespace TradingDaemon.Options;

public class DatabaseObjectNameOptions
{
    public string? ActiveEnvironment { get; set; }
        = "Prod";

    public Dictionary<string, string> Objects { get; set; }
        = new(StringComparer.OrdinalIgnoreCase);

    public Dictionary<string, DatabaseObjectNameEnvironment> Environments { get; set; }
        = new(StringComparer.OrdinalIgnoreCase);
}

public class DatabaseObjectNameEnvironment
{
    public Dictionary<string, string> Objects { get; set; }
        = new(StringComparer.OrdinalIgnoreCase);
}
