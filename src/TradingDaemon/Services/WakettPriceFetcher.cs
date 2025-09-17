using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using TradingDaemon.Data;
using TradingDaemon.Models;

namespace TradingDaemon.Services;

public class WakettPriceFetcher
{
    private readonly WakettApiClient _client;
    private readonly DapperContext _context;
    private readonly IConfiguration _config;
    private readonly ILogger<WakettPriceFetcher> _logger;

    public WakettPriceFetcher(
        WakettApiClient client,
        DapperContext context,
        IConfiguration config,
        ILogger<WakettPriceFetcher> logger)
    {
        _client = client;
        _context = context;
        _config = config;
        _logger = logger;
    }

    public async Task<WakettPriceUploadResult?> FetchAndStoreAsync(
        DateTimeOffset? requestedTimestamp = null,
        CancellationToken cancellationToken = default)
    {
        var baseSymbols = _config
            .GetSection("ExternalApis:WakettApi:Symbols")
            .Get<List<WakettSecuritySymbol>>() ?? new();
        if (baseSymbols.Count == 0)
        {
            _logger.LogWarning("No Wakett symbols configured. Aborting price fetch.");
            return null;
        }

        var missingSymbols = _config
            .GetSection("ExternalApis:WakettApi:MissingSymbols")
            .Get<List<WakettSecuritySymbol>>() ?? new();

        var response = await _client.GetPricesAsync(baseSymbols, requestedTimestamp);
        var computedRates = BuildComputedRates(baseSymbols, missingSymbols, response?.Prices, _logger);
        if (computedRates.Count == 0)
        {
            _logger.LogWarning("Unable to compute any FX rates from Wakett response.");
            return null;
        }

        var timestampUtc = DetermineTimestampUtc(response?.Ts, requestedTimestamp, DateTime.UtcNow);
        _logger.LogInformation("Using timestamp {TimestampUtc} for Wakett price upload.", timestampUtc);

        var securityPairs = await LoadSecurityPairsAsync(
            computedRates.Select(r => r.Definition.SecurityId).Distinct(),
            cancellationToken);

        var uploadItems = new Dictionary<int, WakettPriceUploadItem>();
        var dbRecords = new Dictionary<int, DbPriceRecord>();

        foreach (var rate in computedRates)
        {
            if (!TryParsePair(rate.Definition.Symbol, out var configPair))
            {
                _logger.LogWarning(
                    "Skipping security {SecurityId} because its configured symbol {Symbol} cannot be parsed.",
                    rate.Definition.SecurityId,
                    rate.Definition.Symbol);
                continue;
            }

            var dbPair = securityPairs.TryGetValue(rate.Definition.SecurityId, out var pairFromDb)
                ? pairFromDb
                : configPair;

            if (!TryAdjustRateForSecurity(rate.Rate, configPair, dbPair, out var adjustedRate, out var inverted))
            {
                _logger.LogWarning(
                    "Skipping security {SecurityId} because computed pair {Base}/{Quote} is incompatible with database orientation {DbBase}/{DbQuote}.",
                    rate.Definition.SecurityId,
                    configPair.Base,
                    configPair.Quote,
                    dbPair.Base,
                    dbPair.Quote);
                continue;
            }

            var securityKey = rate.Definition.SecurityId.ToString(CultureInfo.InvariantCulture);
            dbRecords[rate.Definition.SecurityId] = new DbPriceRecord(
                rate.Definition.SecurityId,
                securityKey,
                timestampUtc,
                adjustedRate);

            uploadItems[rate.Definition.SecurityId] = new WakettPriceUploadItem(
                rate.Definition.SecurityId,
                rate.Definition.Symbol,
                adjustedRate,
                inverted);
        }

        if (dbRecords.Count == 0)
        {
            _logger.LogWarning("No database records were produced after adjusting rates.");
            return null;
        }

        await StoreAsync(dbRecords.Values, cancellationToken);

        var ordered = uploadItems.Values
            .OrderBy(i => i.SecurityId)
            .ToList();

        _logger.LogInformation("Uploaded {Count} FX prices to Stage_HistClose.", ordered.Count);
        return new WakettPriceUploadResult(timestampUtc, ordered);
    }

    internal static IReadOnlyList<ComputedRate> BuildComputedRates(
        IEnumerable<WakettSecuritySymbol> baseSymbols,
        IEnumerable<WakettSecuritySymbol> missingSymbols,
        IEnumerable<WakettPrice>? prices,
        ILogger? logger)
    {
        var result = new List<ComputedRate>();
        var graph = new Dictionary<string, Dictionary<string, decimal>>(StringComparer.OrdinalIgnoreCase);

        var priceLookup = new Dictionary<string, WakettPrice>(StringComparer.OrdinalIgnoreCase);
        if (prices is not null)
        {
            foreach (var p in prices)
            {
                var key = NormalizeSymbol(p.Symbol);
                if (string.IsNullOrEmpty(key))
                    continue;
                priceLookup[key] = p;
            }
        }

        foreach (var symbol in baseSymbols)
        {
            if (!TryParsePair(symbol.Symbol, out var pair))
            {
                logger?.LogWarning(
                    "Unable to parse currency pair for Wakett symbol {Symbol}.",
                    symbol.Symbol);
                continue;
            }

            var lookupKey = NormalizeSymbol(symbol.Symbol);
            if (!priceLookup.TryGetValue(lookupKey, out var price))
            {
                logger?.LogWarning("No price returned from Wakett for symbol {Symbol}.", symbol.Symbol);
                continue;
            }

            if (price.Error is not null)
            {
                logger?.LogWarning(
                    "Wakett returned error for symbol {Symbol}: {Code} {Message}.",
                    symbol.Symbol,
                    price.Error.Code,
                    price.Error.Message);
                continue;
            }

            var mid = TryGetMidPrice(price);
            if (!mid.HasValue || mid.Value <= 0m)
            {
                logger?.LogWarning(
                    "Invalid price for symbol {Symbol}. Bid={Bid} Ask={Ask} Mid={Mid}",
                    symbol.Symbol,
                    price.Bid,
                    price.Ask,
                    price.Mid);
                continue;
            }

            AddRate(graph, pair, mid.Value);
            result.Add(new ComputedRate(symbol, pair, mid.Value));
        }

        foreach (var symbol in missingSymbols)
        {
            if (!TryParsePair(symbol.Symbol, out var pair))
            {
                logger?.LogWarning(
                    "Unable to parse currency pair for missing symbol {Symbol}.",
                    symbol.Symbol);
                continue;
            }

            if (TryComputeCrossRate(graph, pair, out var rate))
            {
                AddRate(graph, pair, rate);
                result.Add(new ComputedRate(symbol, pair, rate));
            }
            else
            {
                logger?.LogWarning("Unable to reconstruct price for symbol {Symbol}.", symbol.Symbol);
            }
        }

        return result;
    }

    internal static DateTime DetermineTimestampUtc(
        string? responseTimestamp,
        DateTimeOffset? requestedTimestamp,
        DateTime fallbackUtc)
    {
        if (!string.IsNullOrWhiteSpace(responseTimestamp) &&
            DateTimeOffset.TryParse(
                responseTimestamp,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                out var parsed))
        {
            return parsed.UtcDateTime;
        }

        if (requestedTimestamp.HasValue)
        {
            return requestedTimestamp.Value.UtcDateTime;
        }

        return fallbackUtc;
    }

    internal static string NormalizeSymbol(string symbol)
    {
        if (string.IsNullOrWhiteSpace(symbol))
            return string.Empty;

        var builder = new StringBuilder(symbol.Length);
        foreach (var ch in symbol)
        {
            if (char.IsLetter(ch))
            {
                builder.Append(char.ToUpperInvariant(ch));
            }
        }

        return builder.ToString();
    }

    internal static bool TryParsePair(string? symbol, out CurrencyPair pair)
    {
        pair = default;
        if (string.IsNullOrWhiteSpace(symbol))
            return false;

        var normalized = NormalizeSymbol(symbol);
        if (normalized.Length < 6)
            return false;

        var baseCcy = normalized[..3];
        var quoteCcy = normalized.Substring(3, 3);
        pair = new CurrencyPair(baseCcy, quoteCcy);
        return true;
    }

    internal static decimal? TryGetMidPrice(WakettPrice price)
    {
        if (price.Mid.HasValue)
            return price.Mid.Value;

        if (price.Bid.HasValue && price.Ask.HasValue)
            return (price.Bid.Value + price.Ask.Value) / 2m;

        return price.Bid ?? price.Ask;
    }

    internal static bool TryComputeCrossRate(
        IDictionary<string, Dictionary<string, decimal>> graph,
        CurrencyPair target,
        out decimal rate)
    {
        rate = 0m;
        if (target.Base.Equals(target.Quote, StringComparison.OrdinalIgnoreCase))
        {
            rate = 1m;
            return true;
        }

        if (!graph.ContainsKey(target.Base) || !graph.ContainsKey(target.Quote))
            return false;

        var queue = new Queue<(string Currency, decimal Rate)>();
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            target.Base
        };

        queue.Enqueue((target.Base, 1m));
        while (queue.Count > 0)
        {
            var (currency, currentRate) = queue.Dequeue();
            if (!graph.TryGetValue(currency, out var edges))
                continue;

            foreach (var kvp in edges)
            {
                if (!visited.Add(kvp.Key))
                    continue;

                var nextRate = currentRate * kvp.Value;
                if (kvp.Key.Equals(target.Quote, StringComparison.OrdinalIgnoreCase))
                {
                    rate = nextRate;
                    return true;
                }

                queue.Enqueue((kvp.Key, nextRate));
            }
        }

        return false;
    }

    internal static bool TryAdjustRateForSecurity(
        decimal rate,
        CurrencyPair computedPair,
        CurrencyPair targetPair,
        out decimal adjustedRate,
        out bool inverted)
    {
        if (rate <= 0m)
        {
            adjustedRate = 0m;
            inverted = false;
            return false;
        }

        if (computedPair.Equals(targetPair))
        {
            adjustedRate = rate;
            inverted = false;
            return true;
        }

        if (computedPair.Base.Equals(targetPair.Quote, StringComparison.OrdinalIgnoreCase) &&
            computedPair.Quote.Equals(targetPair.Base, StringComparison.OrdinalIgnoreCase))
        {
            adjustedRate = 1m / rate;
            inverted = true;
            return true;
        }

        adjustedRate = 0m;
        inverted = false;
        return false;
    }

    private static void AddRate(
        IDictionary<string, Dictionary<string, decimal>> graph,
        CurrencyPair pair,
        decimal rate)
    {
        if (rate <= 0m)
            return;

        if (!graph.TryGetValue(pair.Base, out var forward))
        {
            forward = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);
            graph[pair.Base] = forward;
        }
        forward[pair.Quote] = rate;

        if (!graph.TryGetValue(pair.Quote, out var inverse))
        {
            inverse = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);
            graph[pair.Quote] = inverse;
        }
        inverse[pair.Base] = 1m / rate;
    }

    private async Task<Dictionary<int, CurrencyPair>> LoadSecurityPairsAsync(
        IEnumerable<int> securityIds,
        CancellationToken cancellationToken)
    {
        var ids = securityIds.Distinct().ToArray();
        if (ids.Length == 0)
            return new Dictionary<int, CurrencyPair>();

        const string sql = "SELECT SecurityId, BloombergTicker FROM core.Security WHERE SecurityId IN @Ids";
        using var connection = _context.CreateConnection();
        if (connection is DbConnection dbConnection)
        {
            await dbConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            connection.Open();
        }

        var pairs = new Dictionary<int, CurrencyPair>();
        var rows = await connection.QueryAsync<(int SecurityId, string? Ticker)>(sql, new { Ids = ids });
        foreach (var row in rows)
        {
            if (!string.IsNullOrWhiteSpace(row.Ticker) && TryParsePair(row.Ticker, out var pair))
            {
                pairs[row.SecurityId] = pair;
            }
        }

        return pairs;
    }

    private async Task StoreAsync(
        IEnumerable<DbPriceRecord> records,
        CancellationToken cancellationToken)
    {
        var recordList = records.ToList();
        if (recordList.Count == 0)
            return;

        var securityKeys = recordList
            .Select(r => r.SecurityKey)
            .Distinct()
            .ToArray();

        const string deleteSql = "DELETE FROM [Intraday].[mkt].[Stage_HistClose] WHERE BarTimeUtc = @BarTimeUtc AND SecurityId IN @SecurityIds";
        const string insertSql = "INSERT INTO [Intraday].[mkt].[Stage_HistClose] (SecurityId, BarTimeUtc, [Close]) VALUES (@SecurityId, @BarTimeUtc, @Close)";

        using var connection = _context.CreateConnection();
        if (connection is DbConnection dbConnection)
        {
            await dbConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            connection.Open();
        }

        using var transaction = connection.BeginTransaction();
        var parameters = new
        {
            BarTimeUtc = recordList[0].BarTimeUtc,
            SecurityIds = securityKeys
        };

        await connection.ExecuteAsync(deleteSql, parameters, transaction);
        await connection.ExecuteAsync(
            insertSql,
            recordList.Select(r => new { SecurityId = r.SecurityKey, r.BarTimeUtc, Close = r.Close }),
            transaction);

        transaction.Commit();
    }

    internal readonly record struct CurrencyPair(string Base, string Quote);

    internal sealed record ComputedRate(
        WakettSecuritySymbol Definition,
        CurrencyPair Pair,
        decimal Rate);

    private sealed record DbPriceRecord(
        int SecurityId,
        string SecurityKey,
        DateTime BarTimeUtc,
        decimal Close);
}

public sealed record WakettPriceUploadResult(
    DateTime TimestampUtc,
    IReadOnlyList<WakettPriceUploadItem> Prices);

public sealed record WakettPriceUploadItem(
    int SecurityId,
    string Symbol,
    decimal Price,
    bool Inverted);

public sealed record WakettPriceUploadResponse(
    int Uploaded,
    DateTime? TimestampUtc,
    IReadOnlyList<WakettPriceUploadItem> Prices)
{
    public static WakettPriceUploadResponse FromResult(WakettPriceUploadResult result) =>
        new(result.Prices.Count, result.TimestampUtc, result.Prices);

    public static WakettPriceUploadResponse Empty() =>
        new(0, null, Array.Empty<WakettPriceUploadItem>());
}
