using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TradingDaemon.Data;
using TradingDaemon.Models;
using TradingDaemon.Options;
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

    private const string PnlFillsSqlTemplate = @"
WITH FillWithSecurity AS (
    SELECT
        f.ExecuteSize,
        f.ExecutePrice,
        f.CommissionBase,
        f.TradeTimestamp,
        f.SymbolId,
        f.Symbol,
        COALESCE(secById.SecurityId, secBySymbol.SecurityId) AS SecurityId
    FROM {WakettFill} f
    OUTER APPLY (
        SELECT
            TRY_CAST(NULLIF(LTRIM(RTRIM(f.SymbolId)), '') AS bigint) AS SymbolSecurityId,
            UPPER(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(f.Symbol)), '/', ''), '-', ''), '_', ''), ' ', '')) AS NormalizedSymbol
    ) parsed
    LEFT JOIN {IntradayCoreSecurity} secById ON secById.SecurityId = parsed.SymbolSecurityId
    LEFT JOIN {IntradayCoreSecurity} secBySymbol ON secById.SecurityId IS NULL
        AND parsed.NormalizedSymbol IS NOT NULL
        AND UPPER(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(secBySymbol.Symbol)), '/', ''), '-', ''), '_', ''), ' ', '')) = parsed.NormalizedSymbol
    WHERE
        f.TradeTimestamp >= @StartUtc
        AND f.TradeTimestamp < @EndUtc
)
SELECT
    ExecuteSize,
    ExecutePrice,
    CommissionBase,
    SecurityId,
    SymbolId,
    Symbol
FROM FillWithSecurity;
";


    private const string SymbolSqlTemplate = @"SELECT SecurityId, Symbol
FROM {IntradayCoreSecurity}
WHERE IsActive = 1 AND Symbol IS NOT NULL AND LTRIM(RTRIM(Symbol)) <> ''";

    private const string LatestPricesSqlTemplate = @"SELECT SecurityId, [Close]
FROM (
    SELECT
        pb.SecurityId,
        pb.[Close],
        ROW_NUMBER() OVER (PARTITION BY pb.SecurityId ORDER BY pb.BarTimeUtc DESC) AS rn
    FROM {IntradayMarketPriceBar} pb
        WHERE pb.TimeframeMinute = {TimeframeMinute} AND pb.SecurityId IN @SecurityIds
) src
WHERE src.rn = 1;";

    private const string LatestPricesForDaySqlTemplate = @"SELECT SecurityId, [Close]
FROM (
    SELECT
        pb.SecurityId,
        pb.[Close],
        ROW_NUMBER() OVER (PARTITION BY pb.SecurityId ORDER BY pb.BarTimeUtc DESC) AS rn
    FROM {IntradayMarketPriceBar} pb
    WHERE
        pb.TimeframeMinute = {TimeframeMinute}
        AND pb.SecurityId IN @SecurityIds
        AND pb.BarTimeUtc < @EndUtc
) src
WHERE src.rn = 1;";

    private const string PricesAtTimestampSqlTemplate = @"SELECT SecurityId, [Close]
FROM {IntradayMarketPriceBar}
WHERE
    TimeframeMinute = {TimeframeMinute}
    AND SecurityId IN @SecurityIds
    AND BarTimeUtc = @TargetUtc;";

    private const string PositionsSqlTemplate = @"WITH Aggregated AS (
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
    FROM {WakettFill} f
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
    FROM {WakettFill} f
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

    private readonly string _pnlFillsSql;
    private readonly string _symbolSql;
    private readonly string _latestPricesSql;
    private readonly string _latestPricesForDaySql;
    private readonly string _pricesAtTimestampSql;
    private readonly string _positionsSql;
    private readonly string _fillTable;
    private readonly string _securityTable;
    private readonly string _priceBarTable;
    private readonly PriceBarOptions _priceBarOptions;
    private readonly string _timeframeLiteral;

    public PnlReportService(
        DapperContext context,
        ILogger<PnlReportService> logger,
        IDatabaseObjectNameProvider databaseNameProvider,
        IOptions<PriceBarOptions>? priceBarOptions = null)
    {
        _context = context;
        _logger = logger;
        _priceBarOptions = priceBarOptions?.Value ?? new PriceBarOptions();
        _timeframeLiteral = Math.Max(1, _priceBarOptions.TimeframeMinute).ToString(CultureInfo.InvariantCulture);
        _fillTable = databaseNameProvider.GetObjectName(DatabaseObjects.WakettFill);
        _securityTable = databaseNameProvider.GetObjectName(DatabaseObjects.IntradayCoreSecurity);
        _priceBarTable = databaseNameProvider.GetObjectName(DatabaseObjects.IntradayMarketPriceBar);
        _pnlFillsSql = FormatSql(PnlFillsSqlTemplate);
        _symbolSql = FormatSql(SymbolSqlTemplate);
        _latestPricesSql = FormatSql(LatestPricesSqlTemplate);
        _latestPricesForDaySql = FormatSql(LatestPricesForDaySqlTemplate);
        _pricesAtTimestampSql = FormatSql(PricesAtTimestampSqlTemplate);
        _positionsSql = FormatSql(PositionsSqlTemplate);
    }

    public async Task<PnlReport> ComputeAndStoreCurrentDayPnlAsync(DateTime? clock = null, CancellationToken cancellationToken = default)
    {
        var nowUtc = (clock ?? DateTime.UtcNow).ToUniversalTime();
        var nowLocal = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, NewYorkTimeZone);
        var tradingDate = DateOnly.FromDateTime(nowLocal);
        var startLocal = tradingDate.ToDateTime(TimeOnly.MinValue);
        var endLocal = startLocal.AddDays(1);
        var fivePmLocal = startLocal.AddHours(17);
        var startUtc = TimeZoneInfo.ConvertTimeToUtc(startLocal, NewYorkTimeZone);
        var endUtc = TimeZoneInfo.ConvertTimeToUtc(endLocal, NewYorkTimeZone);
        var fivePmUtc = TimeZoneInfo.ConvertTimeToUtc(fivePmLocal, NewYorkTimeZone);

        await using var connection = (SqlConnection)_context.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        await EnsureStorageAsync(connection, cancellationToken);

        var symbolInfos = await LoadSymbolInfosAsync(connection, cancellationToken);
        var fillRows = await LoadPnlFillRowsAsync(connection, startUtc, endUtc, cancellationToken);
        var pnlCalculation = await CalculatePnlAsync(
            connection,
            fillRows,
            symbolInfos,
            fivePmUtc,
            cancellationToken);

        var pnlValue = pnlCalculation.AggregatedUsd ?? (pnlCalculation.PnlByCurrency.TryGetValue("USD", out var usdOnly)
            ? usdOnly
            : 0m);

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

        var positions = await LoadPositionsAsync(connection, startUtc, endUtc, cancellationToken);

        var securityIds = positions
            .Select(p => TryGetSymbolInfo(symbolInfos, p.SymbolId, p.Symbol, null, out var info) ? info.SecurityId : (long?)null)
            .Where(id => id.HasValue)
            .Select(id => id!.Value)
            .Distinct()
            .ToArray();

        var priceLookup = securityIds.Length > 0
            ? await LoadLatestPricesAsync(connection, securityIds, cancellationToken)
            : new Dictionary<long, decimal?>();

        var reportPositions = BuildReportPositions(symbolInfos, priceLookup, positions);

        var grossMarketValue = reportPositions.Sum(p => Math.Abs(p.MarketValueUsd ?? 0m));
        var totalNetExposure = reportPositions.Sum(p => p.MarketValueUsd ?? 0m);

        _logger.LogInformation("Computed current day PnL for {Date}: {PnL}", tradingDate, pnlValue);
        Console.WriteLine("Current day PnL by currency:");
        foreach (var entry in pnlCalculation.PnlByCurrency.OrderBy(e => e.Key, StringComparer.OrdinalIgnoreCase))
        {
            Console.WriteLine($" - {entry.Key}: {entry.Value}");
        }

        if (pnlCalculation.AggregatedUsd.HasValue)
        {
            Console.WriteLine($"Aggregated USD PnL at 5pm NY: {pnlCalculation.AggregatedUsd.Value}");
        }
        else if (pnlCalculation.HasFivePmBar)
        {
            Console.WriteLine("5pm NY conversion rates were partially unavailable; USD aggregation skipped.");
        }
        else
        {
            Console.WriteLine("5pm NY price bars were not available; USD aggregation skipped.");
        }

        return new PnlReport(tradingDate, pnlValue, grossMarketValue, totalNetExposure, reportPositions);
    }

    private static TimeZoneInfo NewYorkTimeZone
        => TimeZoneInfo.FindSystemTimeZoneById(
            OperatingSystem.IsWindows() ? "Eastern Standard Time" : "America/New_York");

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
        var definition = new CommandDefinition(_symbolSql, cancellationToken: cancellationToken);
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

    private async Task<IReadOnlyCollection<PnlFillRow>> LoadPnlFillRowsAsync(IDbConnection connection, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken)
    {
        var definition = new CommandDefinition(
            _pnlFillsSql,
            new { StartUtc = startUtc, EndUtc = endUtc },
            cancellationToken: cancellationToken);
        var rows = await connection.QueryAsync<PnlFillRow>(definition);
        return rows.ToList();
    }

    private async Task<Dictionary<long, decimal?>> LoadLatestPricesAsync(IDbConnection connection, IEnumerable<long> securityIds, CancellationToken cancellationToken)
    {
        var ids = securityIds.Distinct().ToArray();
        if (ids.Length == 0)
        {
            return new Dictionary<long, decimal?>();
        }

        var definition = new CommandDefinition(_latestPricesSql, new { SecurityIds = ids }, cancellationToken: cancellationToken);
        var rows = await connection.QueryAsync<LatestPriceRow>(definition);

        return rows.ToDictionary(row => row.SecurityId, row => row.Close);
    }

    private async Task<Dictionary<long, decimal?>> LoadLatestPricesForDayAsync(IDbConnection connection, IEnumerable<long> securityIds, DateTime endUtc, CancellationToken cancellationToken)
    {
        var ids = securityIds.Distinct().ToArray();
        if (ids.Length == 0)
        {
            return new Dictionary<long, decimal?>();
        }

        var definition = new CommandDefinition(
            _latestPricesForDaySql,
            new { SecurityIds = ids, EndUtc = endUtc },
            cancellationToken: cancellationToken);
        var rows = await connection.QueryAsync<LatestPriceRow>(definition);

        return rows.ToDictionary(row => row.SecurityId, row => row.Close);
    }

    private async Task<IReadOnlyCollection<PositionRow>> LoadPositionsAsync(IDbConnection connection, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken)
    {
        var definition = new CommandDefinition(_positionsSql, new { StartUtc = startUtc, EndUtc = endUtc }, cancellationToken: cancellationToken);
        var rows = await connection.QueryAsync<PositionRow>(definition);
        return rows.ToList();
    }

    private async Task<Dictionary<long, decimal?>> LoadPricesAtTimestampAsync(
        IDbConnection connection,
        IEnumerable<long> securityIds,
        DateTime targetUtc,
        CancellationToken cancellationToken)
    {
        var ids = securityIds.Distinct().ToArray();
        if (ids.Length == 0)
        {
            return new Dictionary<long, decimal?>();
        }

        var definition = new CommandDefinition(
            _pricesAtTimestampSql,
            new { SecurityIds = ids, TargetUtc = targetUtc },
            cancellationToken: cancellationToken);
        var rows = await connection.QueryAsync<LatestPriceRow>(definition);

        return rows.ToDictionary(row => row.SecurityId, row => row.Close);
    }

    private async Task<PnlCalculationResult> CalculatePnlAsync(
        SqlConnection connection,
        IReadOnlyCollection<PnlFillRow> fills,
        IReadOnlyDictionary<string, SymbolInfo> symbolInfos,
        DateTime fivePmUtc,
        CancellationToken cancellationToken)
    {
        if (fills.Count == 0)
        {
            return new PnlCalculationResult(new Dictionary<string, decimal>(), null, false);
        }

        var byCurrency = new Dictionary<string, CurrencyAggregation>(StringComparer.OrdinalIgnoreCase);

        foreach (var fill in fills)
        {
            if (!TryGetSymbolInfo(symbolInfos, fill.SymbolId, fill.Symbol, fill.SecurityId, out var info))
            {
                _logger.LogDebug(
                    "Skipping PnL contribution for unknown symbol {SymbolId} (symbol {Symbol}).",
                    fill.SymbolId,
                    fill.Symbol);
                continue;
            }

            var baseCurrency = info.Pair.QuoteCurrency;
            if (string.IsNullOrWhiteSpace(baseCurrency))
            {
                continue;
            }

            var pnlContribution = (fill.ExecuteSize ?? 0m) * (fill.ExecutePrice ?? 0m);
            var commissionContribution = fill.CommissionBase ?? 0m;

            if (!byCurrency.TryGetValue(baseCurrency, out var aggregation))
            {
                aggregation = new CurrencyAggregation();
                byCurrency[baseCurrency] = aggregation;
            }

            aggregation.PnlBase += pnlContribution;
            aggregation.CommissionBase += commissionContribution;
            aggregation.SecurityIds.Add(info.SecurityId);
        }

        if (byCurrency.Count == 0)
        {
            return new PnlCalculationResult(new Dictionary<string, decimal>(), null, false);
        }

        var securityIds = byCurrency.Values
            .SelectMany(a => a.SecurityIds)
            .Distinct()
            .ToHashSet();

        var usdConversionIds = GetUsdConversionSecurityIds(byCurrency.Keys, symbolInfos.Values.Distinct());
        securityIds.UnionWith(usdConversionIds);

        var priceLookup = securityIds.Count > 0
            ? await LoadPricesAtTimestampAsync(connection, securityIds, fivePmUtc, cancellationToken)
            : new Dictionary<long, decimal?>();

        var hasFivePmBar = priceLookup.Count > 0;
        var pnlByCurrency = byCurrency
            .Select(pair => new KeyValuePair<string, decimal>(pair.Key, pair.Value.PnlBase - pair.Value.CommissionBase))
            .Where(pair => pair.Value != 0m)
            .ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.OrdinalIgnoreCase);

        decimal? totalPnlUsd = null;

        if (hasFivePmBar)
        {
            var conversionGraph = BuildConversionGraph(symbolInfos, priceLookup, Array.Empty<PositionRow>());
            decimal aggregated = 0m;
            var convertedAny = false;

            foreach (var (currency, amount) in pnlByCurrency)
            {
                decimal conversionRate;
                if (string.Equals(currency, "USD", StringComparison.OrdinalIgnoreCase))
                {
                    conversionRate = 1m;
                }
                else if (TryGetConversionRate(currency, "USD", conversionGraph, out var rate) && rate != 0m)
                {
                    conversionRate = rate;
                }
                else
                {
                    _logger.LogWarning(
                        "Unable to convert {Currency} PnL to USD using 5pm rates due to missing data.",
                        currency);
                    continue;
                }

                aggregated += amount * conversionRate;
                convertedAny = true;
            }

            if (convertedAny)
            {
                totalPnlUsd = aggregated;
            }
        }

        return new PnlCalculationResult(pnlByCurrency, totalPnlUsd, hasFivePmBar);
    }

    private IReadOnlyList<PnlReportPosition> BuildReportPositions(
        IReadOnlyDictionary<string, SymbolInfo> symbolInfos,
        IReadOnlyDictionary<long, decimal?> priceLookup,
        IReadOnlyCollection<PositionRow> positions)
    {
        var conversionGraph = BuildConversionGraph(symbolInfos, priceLookup, positions);
        var positionsList = new List<PnlReportPosition>();

        foreach (var position in positions)
        {
            if (!TryGetSymbolInfo(symbolInfos, position.SymbolId, position.Symbol, null, out var info))
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

    private static HashSet<long> GetUsdConversionSecurityIds(
        IEnumerable<string> currencies,
        IEnumerable<SymbolInfo> symbolInfos)
    {
        var ids = new HashSet<long>();

        foreach (var currency in currencies)
        {
            if (string.Equals(currency, "USD", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (TryFindSecurityId(symbolInfos, currency, "USD", out var quoteUsdId))
            {
                ids.Add(quoteUsdId);
                continue;
            }

            if (TryFindSecurityId(symbolInfos, "USD", currency, out var usdBaseId))
            {
                ids.Add(usdBaseId);
            }
        }

        return ids;
    }

    private static bool TryFindSecurityId(
        IEnumerable<SymbolInfo> symbolInfos,
        string baseCurrency,
        string quoteCurrency,
        out long securityId)
    {
        foreach (var info in symbolInfos)
        {
            if (string.Equals(info.Pair.BaseCurrency, baseCurrency, StringComparison.OrdinalIgnoreCase)
                && string.Equals(info.Pair.QuoteCurrency, quoteCurrency, StringComparison.OrdinalIgnoreCase))
            {
                securityId = info.SecurityId;
                return true;
            }
        }

        securityId = default;
        return false;
    }

    private static Dictionary<string, List<(string Target, decimal Rate)>> BuildConversionGraph(
        IReadOnlyDictionary<string, SymbolInfo> symbolInfos,
        IReadOnlyDictionary<long, decimal?> priceLookup,
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
        long? securityId,
        out SymbolInfo info)
    {
        if (securityId.HasValue)
        {
            var normalizedSecurityId = NormalizeSymbol(securityId.Value.ToString(CultureInfo.InvariantCulture));
            if (symbolInfos.TryGetValue(normalizedSecurityId, out info))
            {
                info.Keys.Add(normalizedSecurityId);
                return true;
            }
        }

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

    private string FormatSql(string template)
        => template
            .Replace("{WakettFill}", _fillTable)
            .Replace("{IntradayCoreSecurity}", _securityTable)
            .Replace("{IntradayMarketPriceBar}", _priceBarTable)
            .Replace("{TimeframeMinute}", _timeframeLiteral);

    private sealed record PnlFillRow(
        decimal? ExecuteSize,
        decimal? ExecutePrice,
        decimal? CommissionBase,
        long? SecurityId,
        string? SymbolId,
        string? Symbol);

    private sealed record SecuritySymbolRow(long SecurityId, string Symbol);

    private sealed record LatestPriceRow(long SecurityId, decimal? Close);

    private sealed record PositionRow(string? SymbolId, string? Symbol, decimal NetQuantity, decimal? LastExecutePrice);

    private sealed class CurrencyAggregation
    {
        public decimal PnlBase { get; set; }
        public decimal CommissionBase { get; set; }
        public HashSet<long> SecurityIds { get; } = new();
    }

    private sealed record PnlCalculationResult(
        IReadOnlyDictionary<string, decimal> PnlByCurrency,
        decimal? AggregatedUsd,
        bool HasFivePmBar);

    private sealed class SymbolInfo
    {
        public SymbolInfo(long securityId, CurrencyPair pair)
        {
            SecurityId = securityId;
            Pair = pair;
            Keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        public long SecurityId { get; }

        public CurrencyPair Pair { get; }

        public HashSet<string> Keys { get; }
    }
}
