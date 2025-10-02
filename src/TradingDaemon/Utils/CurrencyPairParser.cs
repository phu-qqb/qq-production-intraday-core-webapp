namespace TradingDaemon.Utils;

internal static class CurrencyPairParser
{
    public static bool TryParse(string? symbol, out CurrencyPair pair)
    {
        pair = default!;

        if (string.IsNullOrWhiteSpace(symbol))
        {
            return false;
        }

        var trimmed = symbol.Trim();
        var normalized = trimmed.ToUpperInvariant();

        string baseCurrency;
        string quoteCurrency;

        if (normalized.Contains('/'))
        {
            var parts = normalized.Split('/');
            if (parts.Length != 2 || parts[0].Length != 3 || parts[1].Length != 3)
            {
                return false;
            }

            baseCurrency = parts[0];
            quoteCurrency = parts[1];
        }
        else if (normalized.Length == 6)
        {
            baseCurrency = normalized[..3];
            quoteCurrency = normalized[3..6];
        }
        else
        {
            return false;
        }

        pair = new CurrencyPair(trimmed, baseCurrency, quoteCurrency);
        return true;
    }
}

internal sealed record CurrencyPair(string OriginalSymbol, string BaseCurrency, string QuoteCurrency)
{
    public string FormattedSymbol => $"{BaseCurrency}/{QuoteCurrency}";
    public string ReversedFormattedSymbol => $"{QuoteCurrency}/{BaseCurrency}";
}
