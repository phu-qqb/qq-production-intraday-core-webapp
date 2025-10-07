using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using TradingDaemon.Data;
using TradingDaemon.Models;
using TradingDaemon.Utils;

namespace TradingDaemon.Services;

public class PnlReportService
{
    private readonly DapperContext _context;
    private readonly ILogger<PnlReportService> _logger;

    private static readonly SemaphoreSlim SchemaSemaphore = new(1, 1);
    private static bool _storageEnsured;

    private const string EnsureSchemaSql = @"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'report')
BEGIN
    EXEC('CREATE SCHEMA [report]');
END";

    private const string EnsureTableSql = @"IF OBJECT_ID('[report].[DailyPnL]', 'U') IS NULL
BEGIN
    CREATE TABLE [report].[DailyPnL]
    (
        TradingDate date NOT NULL CONSTRAINT PK_DailyPnL PRIMARY KEY,
        CalculatedAtUtc datetime2(7) NOT NULL,
        PnL decimal(38, 10) NOT NULL
    );
END";

    private const string UpsertSql = @"MERGE [report].[DailyPnL] AS target
USING (VALUES (@TradingDate, @CalculatedAtUtc, @PnL)) AS source (TradingDate, CalculatedAtUtc, PnL)
    ON target.TradingDate = source.TradingDate
WHEN MATCHED THEN
    UPDATE SET
        CalculatedAtUtc = source.CalculatedAtUtc,
        PnL = source.PnL
WHEN NOT MATCHED THEN
    INSERT (TradingDate, CalculatedAtUtc, PnL)
    VALUES (source.TradingDate, source.CalculatedAtUtc, source.PnL);";

    private const string PnlSql = @"WITH FillWithSecurity AS (
    SELECT
        f.ExecuteSize,
        f.ExecutePrice,
        f.TradeTimestamp,
        f.Side,
        s.SecurityId
    FROM [wakett].[Fill] f
    LEFT JOIN [Intraday].[core].[Security] s ON UPPER(LTRIM(RTRIM(s.Symbol))) = UPPER(LTRIM(RTRIM(f.Symbol)))
), LatestPrices AS (
    SELECT
        pb.SecurityId,
        pb.[Close],
        ROW_NUMBER() OVER (PARTITION BY pb.SecurityId ORDER BY pb.BarTimeUtc DESC) AS rn
    FROM [Intraday].[mkt].[PriceBar] pb
    WHERE
        pb.TimeframeMinute = 60
        AND pb.BarTimeUtc >= @StartUtc
        AND pb.BarTimeUtc < @EndUtc
)
SELECT
    SUM(
        CASE
            WHEN UPPER(f.Side) IN ('SELL', 'S', 'SHORT', 'SS') THEN -1
            WHEN UPPER(f.Side) IN ('BUY', 'B', 'LONG', 'L') THEN 1
            ELSE CASE WHEN f.ExecuteSize < 0 THEN -1 ELSE 1 END
        END
        * COALESCE(f.ExecuteSize, 0)
        * (COALESCE(lp.[Close], f.ExecutePrice) - COALESCE(f.ExecutePrice, 0))
    )
FROM FillWithSecurity f
LEFT JOIN LatestPrices lp ON lp.SecurityId = f.SecurityId AND lp.rn = 1
WHERE
    f.TradeTimestamp >= @StartUtc
    AND f.TradeTimestamp < @EndUtc;";

    private const string SymbolSql = @"SELECT SecurityId, Symbol
FROM [Intraday].[core].[Security]
WHERE IsActive = 1 AND Symbol IS NOT NULL AND LTRIM(RTRIM(Symbol)) <> ''";

    private const string LatestPricesSql = @"SELECT SecurityId, [Close]
FROM (
    SELECT
        pb.SecurityId,
        pb.[Close],
        ROW_NUMBER() OVER (PARTITION BY pb.SecurityId ORDER BY pb.BarTimeUtc DESC) AS rn
    FROM [Intraday].[mkt].[PriceBar] pb
    WHERE pb.TimeframeMinute = 60 AND pb.SecurityId IN @SecurityIds
) src
WHERE src.rn = 1;";

    private const string PositionsSql = @"WITH Aggregated AS (
    SELECT
        f.SymbolId,
        MAX(f.Symbol) AS Symbol,
        SUM(
            CASE
                WHEN UPPER(f.Side) IN ('SELL', 'S', 'SHORT', 'SS') THEN -1
                WHEN UPPER(f.Side) IN ('BUY', 'B', 'LONG', 'L') THEN 1
                ELSE CASE WHEN f.ExecuteSize < 0 THEN -1 ELSE 1 END
            END * COALESCE(f.ExecuteSize, 0)
        ) AS NetQuantity
    FROM [wakett].[Fill] f
    WHERE
        f.TradeTimestamp >= @StartUtc
        AND f.TradeTimestamp < @EndUtc
    GROUP BY f.SymbolId
), LatestFill AS (
    SELECT
        f.SymbolId,
        f.Symbol,
        COALESCE(f.ExecutePrice, 0) AS ExecutePrice,
        ROW_NUMBER() OVER (PARTITION BY f.SymbolId ORDER BY f.TradeTimestamp DESC) AS rn
    FROM [wakett].[Fill] f
    WHERE
        f.TradeTimestamp >= @StartUtc
        AND f.TradeTimestamp < @EndUtc
)
SELECT
    a.SymbolId,
    COALESCE(a.Symbol, lf.Symbol) AS Symbol,
    a.NetQuantity,
    lf.ExecutePrice AS LastExecutePrice
FROM Aggregated a
LEFT JOIN LatestFill lf ON lf.SymbolId = a.SymbolId AND lf.rn = 1
WHERE a.NetQuantity IS NOT NULL AND a.NetQuantity <> 0;";

    public PnlReportService(DapperContext context, ILogger<PnlReportService> logger)
    {
        _context = context;
        _logger = logger;
    }

    public async Task<PnlReport> ComputeAndStoreCurrentDayPnlAsync(DateTime? clock = null, CancellationToken cancellationToken = default)
    {
        var nowUtc = (clock ?? DateTime.UtcNow).ToUniversalTime();
        var tradingDate = DateOnly.FromDateTime(nowUtc);
        var startUtc = tradingDate.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc);
        var endUtc = startUtc.AddDays(1);

        await using var connection = (SqlConnection)_context.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        await EnsureStorageAsync(connection, cancellationToken);

        var pnl = await connection.ExecuteScalarAsync<decimal?>(
            new CommandDefinition(PnlSql, new { StartUtc = startUtc, EndUtc = endUtc }, cancellationToken: cancellationToken));

        var pnlValue = pnl ?? 0m;

        await connection.ExecuteAsync(
            new CommandDefinition(
                UpsertSql,
                new
                {
                    TradingDate = tradingDate.ToDateTime(TimeOnly.MinValue),
                    CalculatedAtUtc = nowUtc,
                    PnL = pnlValue
                },
                cancellationToken: cancellationToken));

        var symbolInfos = await LoadSymbolInfosAsync(connection, cancellationToken);
        var positions = await LoadPositionsAsync(connection, startUtc, endUtc, cancellationToken);

        var securityIds = positions
            .Select(p => TryGetSymbolInfo(symbolInfos, p.SymbolId, p.Symbol, out var info) ? info.SecurityId : (int?)null)
            .Where(id => id.HasValue)
            .Select(id => id!.Value)
            .Distinct()
            .ToArray();

        var priceLookup = securityIds.Length > 0
            ? await LoadLatestPricesAsync(connection, securityIds, cancellationToken)
            : new Dictionary<int, decimal?>();

        var reportPositions = BuildReportPositions(symbolInfos, priceLookup, positions);

        var grossMarketValue = reportPositions.Sum(p => Math.Abs(p.MarketValueUsd ?? 0m));
        var totalNetExposure = reportPositions.Sum(p => p.MarketValueUsd ?? 0m);

        _logger.LogInformation("Computed current day PnL for {Date}: {PnL}", tradingDate, pnlValue);
        Console.WriteLine($"Current day PnL: {pnlValue}");

        return new PnlReport(tradingDate, pnlValue, grossMarketValue, totalNetExposure, reportPositions);
    }

    private async Task EnsureStorageAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        if (_storageEnsured)
        {
            return;
        }

        await SchemaSemaphore.WaitAsync(cancellationToken);
        try
        {
            if (_storageEnsured)
            {
                return;
            }

            await connection.ExecuteAsync(new CommandDefinition(EnsureSchemaSql, cancellationToken: cancellationToken));
            await connection.ExecuteAsync(new CommandDefinition(EnsureTableSql, cancellationToken: cancellationToken));

            _storageEnsured = true;
        }
        finally
        {
            SchemaSemaphore.Release();
        }
    }

    private async Task<Dictionary<string, SymbolInfo>> LoadSymbolInfosAsync(IDbConnection connection, CancellationToken cancellationToken)
    {
        var definition = new CommandDefinition(SymbolSql, cancellationToken: cancellationToken);
        var rows = await connection.QueryAsync<SecuritySymbolRow>(definition);

        var result = new Dictionary<string, SymbolInfo>(StringComparer.OrdinalIgnoreCase);
        foreach (var row in rows)
        {
            if (string.IsNullOrWhiteSpace(row.Symbol))
            {
                continue;
            }

            var normalized = NormalizeSymbol(row.Symbol);
            if (string.IsNullOrEmpty(normalized))
            {
                continue;
            }

            if (!CurrencyPairParser.TryParse(row.Symbol, out var pair))
            {
                continue;
            }

            var info = new SymbolInfo(row.SecurityId, pair);
            info.Keys.Add(normalized);
            result[normalized] = info;

            var securityKey = NormalizeSymbol(row.SecurityId.ToString(CultureInfo.InvariantCulture));
            info.Keys.Add(securityKey);
            result[securityKey] = info;

            var formattedKey = NormalizeSymbol(info.Pair.FormattedSymbol);
            if (!string.IsNullOrEmpty(formattedKey))
            {
                info.Keys.Add(formattedKey);
                result[formattedKey] = info;
            }
        }

        return result;
    }

    private async Task<Dictionary<int, decimal?>> LoadLatestPricesAsync(IDbConnection connection, IEnumerable<int> securityIds, CancellationToken cancellationToken)
    {
        var ids = securityIds.Distinct().ToArray();
        if (ids.Length == 0)
        {
            return new Dictionary<int, decimal?>();
        }

        var definition = new CommandDefinition(LatestPricesSql, new { SecurityIds = ids }, cancellationToken: cancellationToken);
        var rows = await connection.QueryAsync<LatestPriceRow>(definition);

        return rows.ToDictionary(row => row.SecurityId, row => row.Close);
    }

    private async Task<IReadOnlyCollection<PositionRow>> LoadPositionsAsync(IDbConnection connection, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken)
    {
        var definition = new CommandDefinition(PositionsSql, new { StartUtc = startUtc, EndUtc = endUtc }, cancellationToken: cancellationToken);
        var rows = await connection.QueryAsync<PositionRow>(definition);
        return rows.ToList();
    }

    private IReadOnlyList<PnlReportPosition> BuildReportPositions(
        IReadOnlyDictionary<string, SymbolInfo> symbolInfos,
        IReadOnlyDictionary<int, decimal?> priceLookup,
        IReadOnlyCollection<PositionRow> positions)
    {
        var conversionGraph = BuildConversionGraph(symbolInfos, priceLookup, positions);
        var positionsList = new List<PnlReportPosition>();

        foreach (var position in positions)
        {
            if (!TryGetSymbolInfo(symbolInfos, position.SymbolId, position.Symbol, out var info))
            {
                _logger.LogDebug(
                    "Skipping position for unknown symbol {SymbolId} (symbol {Symbol}).",
                    position.SymbolId,
                    position.Symbol);
                continue;
            }

            var price = priceLookup.TryGetValue(info.SecurityId, out var latest) && latest.HasValue && latest.Value != 0m
                ? latest.Value
                : (position.LastExecutePrice is { } last && last != 0m ? last : (decimal?)null);

            decimal? marketValueUsd = null;

            if (TryGetConversionRate(info.Pair.BaseCurrency, "USD", conversionGraph, out var baseToUsd))
            {
                marketValueUsd = position.NetQuantity * baseToUsd;
            }
            else if (price.HasValue && TryGetConversionRate(info.Pair.QuoteCurrency, "USD", conversionGraph, out var quoteToUsd))
            {
                var quoteAmount = -position.NetQuantity * price.Value;
                marketValueUsd = quoteAmount * quoteToUsd;
            }

            positionsList.Add(new PnlReportPosition(
                info.Pair.FormattedSymbol,
                info.Pair.BaseCurrency,
                info.Pair.QuoteCurrency,
                position.NetQuantity,
                price,
                marketValueUsd));
        }

        return positionsList
            .OrderByDescending(p => Math.Abs(p.MarketValueUsd ?? 0m))
            .ThenBy(p => p.Symbol, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static Dictionary<string, List<(string Target, decimal Rate)>> BuildConversionGraph(
        IReadOnlyDictionary<string, SymbolInfo> symbolInfos,
        IReadOnlyDictionary<int, decimal?> priceLookup,
        IReadOnlyCollection<PositionRow> positions)
    {
        var graph = new Dictionary<string, List<(string Target, decimal Rate)>>(StringComparer.OrdinalIgnoreCase);
        var lastPriceLookup = BuildLastPriceLookup(positions);

        foreach (var info in symbolInfos.Values.Distinct())
        {
            decimal? price = null;

            if (priceLookup.TryGetValue(info.SecurityId, out var close) && close.HasValue && close.Value != 0m)
            {
                price = close.Value;
            }
            else
            {
                price = info.Keys
                    .Select(key => lastPriceLookup.TryGetValue(key, out var last) ? last : null)
                    .FirstOrDefault(last => last.HasValue);
            }

            if (!price.HasValue || price.Value == 0m)
            {
                continue;
            }

            var resolvedPrice = price.Value;

            AddEdge(graph, info.Pair.BaseCurrency, info.Pair.QuoteCurrency, resolvedPrice);
            AddEdge(graph, info.Pair.QuoteCurrency, info.Pair.BaseCurrency, 1m / resolvedPrice);
        }

        graph.TryAdd("USD", new List<(string, decimal)>());
        return graph;
    }

    private static Dictionary<string, decimal?> BuildLastPriceLookup(IReadOnlyCollection<PositionRow> positions)
    {
        var lookup = new Dictionary<string, decimal?>(StringComparer.OrdinalIgnoreCase);

        foreach (var position in positions)
        {
            if (position.LastExecutePrice is not { } price || price == 0m)
            {
                continue;
            }

            if (!string.IsNullOrWhiteSpace(position.SymbolId))
            {
                lookup[NormalizeSymbol(position.SymbolId)] = price;
            }

            if (!string.IsNullOrWhiteSpace(position.Symbol))
            {
                lookup[NormalizeSymbol(position.Symbol)] = price;
            }
        }

        return lookup;
    }

    private static bool TryGetSymbolInfo(
        IReadOnlyDictionary<string, SymbolInfo> symbolInfos,
        string? symbolId,
        string? symbol,
        out SymbolInfo info)
    {
        if (!string.IsNullOrWhiteSpace(symbolId))
        {
            var normalizedId = NormalizeSymbol(symbolId);
            if (symbolInfos.TryGetValue(normalizedId, out info))
            {
                info.Keys.Add(normalizedId);
                return true;
            }
        }

        if (!string.IsNullOrWhiteSpace(symbol))
        {
            var normalizedSymbol = NormalizeSymbol(symbol);
            if (symbolInfos.TryGetValue(normalizedSymbol, out info))
            {
                info.Keys.Add(normalizedSymbol);
                return true;
            }
        }

        info = default!;
        return false;
    }

    private static string NormalizeSymbol(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? string.Empty : value.Trim().ToUpperInvariant();
    }

    private static void AddEdge(
        IDictionary<string, List<(string Target, decimal Rate)>> graph,
        string from,
        string to,
        decimal rate)
    {
        if (rate == 0m)
        {
            return;
        }

        if (!graph.TryGetValue(from, out var edges))
        {
            edges = new List<(string Target, decimal Rate)>();
            graph[from] = edges;
        }

        edges.Add((to, rate));
    }

    private static bool TryGetConversionRate(
        string source,
        string target,
        IReadOnlyDictionary<string, List<(string Target, decimal Rate)>> graph,
        out decimal rate)
    {
        if (string.Equals(source, target, StringComparison.OrdinalIgnoreCase))
        {
            rate = 1m;
            return true;
        }

        if (!graph.TryGetValue(source, out var initialEdges) || initialEdges.Count == 0)
        {
            rate = 0m;
            return false;
        }

        var queue = new Queue<(string Currency, decimal Accumulated)>();
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { source };

        foreach (var edge in initialEdges)
        {
            queue.Enqueue((edge.Target, edge.Rate));
        }

        while (queue.Count > 0)
        {
            var (currency, accumulated) = queue.Dequeue();
            if (!visited.Add(currency))
            {
                continue;
            }

            if (string.Equals(currency, target, StringComparison.OrdinalIgnoreCase))
            {
                rate = accumulated;
                return true;
            }

            if (!graph.TryGetValue(currency, out var edges))
            {
                continue;
            }

            foreach (var edge in edges)
            {
                queue.Enqueue((edge.Target, accumulated * edge.Rate));
            }
        }

        rate = 0m;
        return false;
    }

    private sealed record SecuritySymbolRow
    {
        public int SecurityId { get; init; }

        public string? Symbol { get; init; }
    }

    private sealed record LatestPriceRow
    {
        public int SecurityId { get; init; }

        public decimal? Close { get; init; }
    }

    private sealed record PositionRow(string? SymbolId, string? Symbol, decimal NetQuantity, decimal? LastExecutePrice);

    private sealed class SymbolInfo
    {
        public SymbolInfo(int securityId, CurrencyPair pair)
        {
            SecurityId = securityId;
            Pair = pair;
            Keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        public int SecurityId { get; }

        public CurrencyPair Pair { get; }

        public HashSet<string> Keys { get; }
    }
}
