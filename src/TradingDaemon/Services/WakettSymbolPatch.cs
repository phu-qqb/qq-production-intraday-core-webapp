using System.Collections.Generic;

namespace TradingDaemon.Services;

internal static class WakettSymbolPatch
{
    private static readonly IReadOnlyDictionary<int, string> SymbolOverrides = new Dictionary<int, string>
    {
        [127] = "AUD/USD",
        [134] = "EUR/GBP",
        [136] = "USD/CHF",
    };

    public static string GetRequestSymbol(int securityId, string configuredSymbol)
    {
        if (SymbolOverrides.TryGetValue(securityId, out var overrideSymbol) &&
            !string.IsNullOrWhiteSpace(overrideSymbol))
        {
            return overrideSymbol;
        }

        return configuredSymbol;
    }
}
