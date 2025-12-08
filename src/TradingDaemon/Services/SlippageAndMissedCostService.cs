using System.Globalization;
using System.Linq;
using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TradingDaemon.Data;
using TradingDaemon.Models;
using TradingDaemon.Options;
using TradingDaemon.Utils;

namespace TradingDaemon.Services;

public sealed class SlippageAndMissedCostService
{
    private readonly DapperContext _context;
    private readonly ILogger<SlippageAndMissedCostService> _logger;
    private readonly string _orderTable;
    private readonly string _fillTable;
    private readonly string _priceBarView;
    private readonly string _timeframeLiteral;
    private readonly int _timeframeMinutes;

    private const decimal TradingCostUsdPerUsdNotional = 5m / 1_000_000m;

    private const string OrdersSqlTemplate = @"SELECT
    WakettOrderId,
    Symbol,
    Side,
    SizeValue,
    Aum,
    ScheduledTimestamp
FROM {WakettOrder}
WHERE ScheduledTimestamp >= @StartUtc AND ScheduledTimestamp < @EndUtc";

    private const string FillsSqlTemplate = @"SELECT
    WakettFillId,
    Symbol,
    Side,
    ExecuteSize,
    ExecutePrice,
    ExecuteTimestamp
FROM {WakettFill}
WHERE TradeTimestamp >= @StartUtc AND TradeTimestamp < @EndUtc";

    private const string PriceBarsSqlTemplate = @"SELECT
    Symbol,
    BarTimeUtc,
    [Close]
FROM {IntradayMarketPriceBarView}
WHERE TimeframeMinute = {TimeframeMinute}
    AND BarTimeUtc >= @StartUtc AND BarTimeUtc <= @EndUtc
    AND Symbol IN @Symbols
ORDER BY Symbol, BarTimeUtc";

    private static readonly DateOnly HistoricalFillStartDate = new(2025, 12, 1);

    public SlippageAndMissedCostService(
        DapperContext context,
        ILogger<SlippageAndMissedCostService> logger,
        IDatabaseObjectNameProvider databaseObjectNameProvider,
        IOptions<PriceBarOptions>? priceBarOptions = null)
    {
        _context = context;
        _logger = logger;
        var options = priceBarOptions?.Value ?? new PriceBarOptions();
        _timeframeMinutes = Math.Max(1, options.TimeframeMinute);
        _timeframeLiteral = _timeframeMinutes.ToString(CultureInfo.InvariantCulture);
        _orderTable = databaseObjectNameProvider.GetObjectName(DatabaseObjects.WakettOrder);
        _fillTable = databaseObjectNameProvider.GetObjectName(DatabaseObjects.WakettFill);
        _priceBarView = databaseObjectNameProvider.GetObjectName(DatabaseObjects.IntradayMarketPriceBarView);
    }

    public async Task<SlippageResult> ComputeAsync(SlippageRequest request, CancellationToken cancellationToken)
    {
        var tradingDate = DateOnly.FromDateTime(request.Date.Date);
        var startLocal = tradingDate.ToDateTime(TimeOnly.MinValue);
        var endLocal = startLocal.AddDays(1);
        var startUtc = TimeZoneInfo.ConvertTimeToUtc(startLocal, NewYorkTimeZone);
        var endUtc = TimeZoneInfo.ConvertTimeToUtc(endLocal, NewYorkTimeZone);
        var previousWeekdayLocal = GetPreviousWeekday(tradingDate).ToDateTime(TimeOnly.MinValue);
        var priceBarStartUtc = TimeZoneInfo.ConvertTimeToUtc(previousWeekdayLocal, NewYorkTimeZone);
        var historicalFillStartUtc = TimeZoneInfo.ConvertTimeToUtc(HistoricalFillStartDate.ToDateTime(TimeOnly.MinValue), NewYorkTimeZone);
        var historicalFillEndUtc = startUtc;

        using var connection = _context.CreateConnection();

        var orders = (await connection.QueryAsync<OrderRow>(
                new CommandDefinition(FormatSql(OrdersSqlTemplate), new { StartUtc = startUtc, EndUtc = endUtc }, cancellationToken: cancellationToken)))
            .Where(o => !string.IsNullOrWhiteSpace(o.Symbol))
            .ToList();

        var fills = (await connection.QueryAsync<FillRow>(
                new CommandDefinition(FormatSql(FillsSqlTemplate), new { StartUtc = startUtc, EndUtc = endUtc }, cancellationToken: cancellationToken)))
            .Where(f => !string.IsNullOrWhiteSpace(f.Symbol))
            .ToList();

        var historicalFills = (await connection.QueryAsync<FillRow>(
                new CommandDefinition(
                    FormatSql(FillsSqlTemplate),
                    new { StartUtc = historicalFillStartUtc, EndUtc = historicalFillEndUtc },
                    cancellationToken: cancellationToken)))
            .Where(f => !string.IsNullOrWhiteSpace(f.Symbol))
            .ToList();

        var startingPositions = CalculatePositionsAtStartOfDay(historicalFills);

        var symbolQueries = BuildSymbolQueries(
                orders.Select(o => o.Symbol)
                    .Concat(fills.Select(f => f.Symbol))
                    .Concat(startingPositions.Keys))
            .ToList();

        var symbolSet = symbolQueries
            .Select(q => q.QuerySymbol)
            .Where(s => s.Length > 0)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (symbolSet.Length == 0)
        {
            _logger.LogWarning("No symbols found for slippage computation on {Date}", tradingDate);
            return new SlippageResult(
                tradingDate,
                new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase),
                new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase),
                null,
                null,
                null,
                false);
        }

        var priceBarsDefinition = new CommandDefinition(
            FormatSql(PriceBarsSqlTemplate),
            new { StartUtc = priceBarStartUtc, EndUtc = endUtc, Symbols = symbolSet },
            cancellationToken: cancellationToken);

        var priceBars = await connection.QueryAsync<PriceBarRow>(priceBarsDefinition);
        var priceBarsByQuery = priceBars
            .GroupBy(b => NormalizeSymbol(b.Symbol))
            .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.OrdinalIgnoreCase);

        var symbolQueriesByQuery = symbolQueries
            .GroupBy(q => q.QuerySymbol)
            .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);

        var adjustedPriceBars = new List<PriceBarRow>();

        foreach (var (querySymbol, bars) in priceBarsByQuery)
        {
            var adjustedSymbol = querySymbol;
            var invertPrices = false;

            if (symbolQueriesByQuery.TryGetValue(querySymbol, out var query))
            {
                adjustedSymbol = query.TargetSymbol;
                invertPrices = query.InvertPrice;
            }

            foreach (var bar in bars)
            {
                var close = bar.Close;

                if (invertPrices && close != 0m)
                {
                    close = 1m / close;
                }

                adjustedPriceBars.Add(new PriceBarRow(adjustedSymbol, bar.BarTimeUtc, close));
            }
        }

        var barsBySymbol = adjustedPriceBars
            .GroupBy(b => NormalizeSymbol(b.Symbol))
            .ToDictionary(g => g.Key, g => g.OrderBy(b => b.BarTimeUtc).ToList(), StringComparer.OrdinalIgnoreCase);

        var theoreticalPnlByCurrency = CalculateTheoreticalPnlByCurrency(orders, barsBySymbol);
        var lastClosePrices = ExtractLastClosePrices(barsBySymbol);
        var previousClosePrices = ExtractLastClosePricesBeforeTimestamp(barsBySymbol, startUtc);
        var conversionGraph = BuildConversionGraph(lastClosePrices);
        var hasConversionPrices = conversionGraph.Count > 0;

        var realPnlFills = AddVirtualStartingFills(fills, startingPositions, previousClosePrices, startUtc);
        var realPnlResult = CalculateRealPnlByCurrency(realPnlFills, lastClosePrices, conversionGraph);
        var realPnlByCurrency = realPnlResult.Totals;

        var theoreticalUsd = theoreticalPnlByCurrency.TryGetValue("USD", out var theoreticalUsdTotal)
            ? theoreticalUsdTotal
            : (decimal?)null;
        var realUsd = hasConversionPrices
            ? AggregateToUsd(realPnlByCurrency, conversionGraph)
            : null;

        if (realUsd.HasValue)
        {
            realUsd -= realPnlResult.TotalTradingCostUsd;
        }

        var slippageCost = theoreticalUsd.HasValue && realUsd.HasValue
            ? realUsd.Value - theoreticalUsd.Value
            : (decimal?)null;

        Console.WriteLine($"Slippage computation for {tradingDate:yyyy-MM-dd}");
        Console.WriteLine("Theoretical PnL by currency:");
        foreach (var entry in theoreticalPnlByCurrency.OrderBy(e => e.Key, StringComparer.OrdinalIgnoreCase))
        {
            Console.WriteLine($" - {entry.Key}: {entry.Value}");
        }

        Console.WriteLine("Real PnL by currency:");
        foreach (var entry in realPnlByCurrency.OrderBy(e => e.Key, StringComparer.OrdinalIgnoreCase))
        {
            Console.WriteLine($" - {entry.Key}: {entry.Value}");
        }

        if (slippageCost.HasValue || theoreticalUsd.HasValue || realUsd.HasValue || realPnlResult.TotalTradingCostUsd != 0m)
        {
            Console.WriteLine("Aggregated USD values using last available close prices:");
            Console.WriteLine(theoreticalUsd.HasValue
                ? $" - Theoretical PnL (USD): {theoreticalUsd.Value}"
                : " - Theoretical PnL could not be fully converted to USD.");
            Console.WriteLine(realUsd.HasValue
                ? $" - Real PnL (USD, after trading costs): {realUsd.Value}"
                : " - Real PnL could not be fully converted to USD.");
            Console.WriteLine($" - Total trading cost (USD): {realPnlResult.TotalTradingCostUsd}");
            Console.WriteLine(slippageCost.HasValue
                ? $" - Slippage and missed trade cost (USD): {slippageCost.Value}"
                : " - Slippage and missed trade cost could not be aggregated to USD.");
        }
        else if (hasConversionPrices)
        {
            Console.WriteLine("Conversion rates were partially unavailable; USD aggregation skipped.");
        }
        else
        {
            Console.WriteLine("Price bars were not available; USD aggregation skipped.");
        }

        var missedTrades = IdentifyMissedTrades(orders, fills, barsBySymbol, _timeframeMinutes);
        Console.WriteLine($"Missed trades for {tradingDate:yyyy-MM-dd}:");
        if (missedTrades.Count == 0)
        {
            Console.WriteLine(" - None");
        }
        else
        {
            foreach (var trade in missedTrades.OrderBy(m => m.Symbol, StringComparer.OrdinalIgnoreCase)
                         .ThenBy(m => m.BarTimeUtc))
            {
                Console.WriteLine(
                    $" - {trade.Symbol} at {trade.BarTimeUtc:HH:mm} UTC | Target: {trade.TargetSize}, Filled: {trade.FilledSize}, " +
                    $"Diff: {trade.SizeDifference}, Price Δ: {trade.PriceDelta}, Missed PnL: {trade.MissedPnl}");
            }
        }

        return new SlippageResult(
            tradingDate,
            theoreticalPnlByCurrency,
            realPnlByCurrency,
            theoreticalUsd,
            realUsd,
            slippageCost,
            hasConversionPrices);
    }

    private Dictionary<string, decimal> CalculateTheoreticalPnlByCurrency(
        IReadOnlyCollection<OrderRow> orders,
        IReadOnlyDictionary<string, List<PriceBarRow>> barsBySymbol)
    {
        var totals = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);

        foreach (var order in orders)
        {
            if (!CurrencyPairParser.TryParse(order.Symbol, out var pair))
            {
                _logger.LogDebug("Skipping theoretical PnL for unparsable symbol {Symbol}", order.Symbol);
                continue;
            }

            if (string.IsNullOrWhiteSpace(pair.QuoteCurrency))
            {
                continue;
            }

            var normalizedSymbol = NormalizeSymbol(order.Symbol);
            if (!barsBySymbol.TryGetValue(normalizedSymbol, out var bars) || bars.Count < 2)
            {
                _logger.LogDebug("No price bars for symbol {Symbol} to compute theoretical PnL", order.Symbol);
                continue;
            }

            var barIndex = bars.FindIndex(b => b.BarTimeUtc == order.ScheduledTimestamp.UtcDateTime);
            if (barIndex < 0 || barIndex >= bars.Count - 1)
            {
                _logger.LogDebug("No matching bar for order at {Timestamp} ({Symbol})", order.ScheduledTimestamp, pair.FormattedSymbol);
                continue;
            }

            var currentBar = bars[barIndex];
            var nextBar = bars[barIndex + 1];

            if (currentBar.Close == 0m)
            {
                continue;
            }

            var priceReturn = (nextBar.Close - currentBar.Close) / currentBar.Close;
            var sideMultiplier = GetSideMultiplier(order.Side);
            var notionalUsd = (order.SizeValue ?? 0m) * (order.Aum ?? 0m) * sideMultiplier;

            var pnlQuote = notionalUsd * priceReturn;

            if (pnlQuote != 0m)
            {
                var currency = string.Equals(pair.QuoteCurrency, "USD", StringComparison.OrdinalIgnoreCase)
                    ? pair.QuoteCurrency
                    : "USD";

                totals[currency] = totals.TryGetValue(currency, out var existing)
                    ? existing + pnlQuote
                    : pnlQuote;
            }
        }

        return totals;
    }

    private static IReadOnlyDictionary<string, decimal> CalculatePositionsAtStartOfDay(
        IReadOnlyCollection<FillRow> historicalFills)
    {
        var positions = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);

        var fillsBySymbol = historicalFills
            .GroupBy(f => NormalizeSymbol(f.Symbol))
            .ToDictionary(g => g.Key, g => g.OrderBy(f => f.ExecuteTimestamp).ToList(), StringComparer.OrdinalIgnoreCase);

        foreach (var (symbol, symbolFills) in fillsBySymbol)
        {
            var position = 0m;

            foreach (var fill in symbolFills)
            {
                var multiplier = GetCashFlowSideMultiplier(fill.Side);
                var executeSize = fill.ExecuteSize ?? 0m;

                position += executeSize * multiplier;
            }

            if (position != 0m)
            {
                positions[symbol] = position;
            }
        }

        return positions;
    }

    private RealPnlComputationResult CalculateRealPnlByCurrency(
        IReadOnlyCollection<FillRow> fills,
        IReadOnlyDictionary<string, decimal> lastClosePricesBySymbol,
        IReadOnlyDictionary<string, List<(string Target, decimal Rate)>> conversionGraph)
    {
        var totals = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);
        var totalTradingCostUsd = 0m;

        var fillsBySymbol = fills
            .GroupBy(f => NormalizeSymbol(f.Symbol))
            .ToDictionary(g => g.Key, g => g.OrderBy(f => f.ExecuteTimestamp).ToList(), StringComparer.OrdinalIgnoreCase);

        foreach (var (symbol, symbolFills) in fillsBySymbol)
        {
            if (!CurrencyPairParser.TryParse(symbol, out var pair))
            {
                continue;
            }

            if (string.IsNullOrWhiteSpace(pair.QuoteCurrency))
            {
                continue;
            }

            if (!lastClosePricesBySymbol.TryGetValue(symbol, out var lastClose) || lastClose == 0m)
            {
                continue;
            }

            var position = 0m;
            var pnlQuote = 0m;

            foreach (var fill in symbolFills)
            {
                var multiplier = GetCashFlowSideMultiplier(fill.Side);
                var executeSize = fill.ExecuteSize ?? 0m;
                var executePrice = fill.ExecutePrice ?? 0m;

                pnlQuote += executePrice * executeSize * multiplier;
                position += executeSize * multiplier;

                var notionalQuote = Math.Abs(executePrice * executeSize);
                if (TryConvertToUsd(notionalQuote, pair.QuoteCurrency, conversionGraph, out var notionalUsd))
                {
                    totalTradingCostUsd += notionalUsd * TradingCostUsdPerUsdNotional;
                }
            }

            if (position != 0m)
            {
                var flattenMultiplier = position > 0m ? -1m : 1m;
                pnlQuote += lastClose * Math.Abs(position) * flattenMultiplier;
            }

            if (pnlQuote != 0m)
            {
                totals[pair.QuoteCurrency] = totals.TryGetValue(pair.QuoteCurrency, out var existing)
                    ? existing + pnlQuote
                    : pnlQuote;
            }

        }

        return new RealPnlComputationResult(totals, totalTradingCostUsd);
    }

    private IReadOnlyCollection<FillRow> AddVirtualStartingFills(
        IReadOnlyCollection<FillRow> fills,
        IReadOnlyDictionary<string, decimal> startingPositions,
        IReadOnlyDictionary<string, decimal> previousClosePrices,
        DateTime startUtc)
    {
        if (startingPositions.Count == 0)
        {
            return fills;
        }

        var augmented = fills.ToList();
        var startTimestamp = new DateTimeOffset(startUtc, TimeSpan.Zero);

        foreach (var (symbol, position) in startingPositions)
        {
            if (position == 0m)
            {
                continue;
            }

            if (!previousClosePrices.TryGetValue(symbol, out var previousClose) || previousClose == 0m)
            {
                continue;
            }

            var side = position > 0m ? "SELL" : "BUY";

            augmented.Add(new FillRow(
                WakettFillId: 0,
                Symbol: symbol,
                Side: side,
                ExecuteSize: Math.Abs(position),
                ExecutePrice: previousClose,
                ExecuteTimestamp: startTimestamp));
        }

        return augmented;
    }

    private static decimal GetCashFlowSideMultiplier(string? side)
    {
        if (string.IsNullOrWhiteSpace(side))
        {
            return 1m;
        }

        var normalized = side.Trim().ToUpperInvariant();
        return normalized.StartsWith("B", StringComparison.Ordinal) ? -1m : 1m;
    }

    private static decimal GetSideMultiplier(string? side)
    {
        if (string.IsNullOrWhiteSpace(side))
        {
            return 1m;
        }

        var normalized = side.Trim().ToUpperInvariant();
        return normalized.StartsWith("S", StringComparison.Ordinal) ? -1m : 1m;
    }

    private static IReadOnlyDictionary<string, decimal> ExtractPricesAtTimestamp(
        IReadOnlyDictionary<string, List<PriceBarRow>> barsBySymbol,
        DateTime targetUtc)
    {
        var result = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);

        foreach (var (symbol, bars) in barsBySymbol)
        {
            var matching = bars.FirstOrDefault(b => b.BarTimeUtc == targetUtc);

            if (matching is not null && matching.Close != 0m)
            {
                result[symbol] = matching.Close;
            }
        }

        return result;
    }

    private static IReadOnlyDictionary<string, decimal> ExtractLastClosePricesBeforeTimestamp(
        IReadOnlyDictionary<string, List<PriceBarRow>> barsBySymbol,
        DateTime targetUtc)
    {
        var result = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);

        foreach (var (symbol, bars) in barsBySymbol)
        {
            var lastBefore = bars
                .Where(b => b.BarTimeUtc < targetUtc)
                .OrderBy(b => b.BarTimeUtc)
                .LastOrDefault();

            if (lastBefore != null)
            {
                result[symbol] = lastBefore.Close;
            }
        }

        return result;
    }

    private static IReadOnlyDictionary<string, decimal> ExtractLastClosePrices(
        IReadOnlyDictionary<string, List<PriceBarRow>> barsBySymbol)
    {
        var result = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);

        foreach (var (symbol, bars) in barsBySymbol)
        {
            var lastBar = bars.LastOrDefault(b => b.Close != 0m) ?? bars.LastOrDefault();

            if (lastBar is not null && lastBar.Close != 0m)
            {
                result[symbol] = lastBar.Close;
            }
        }

        return result;
    }

    private static IReadOnlyCollection<MissedTrade> IdentifyMissedTrades(
        IReadOnlyCollection<OrderRow> orders,
        IReadOnlyCollection<FillRow> fills,
        IReadOnlyDictionary<string, List<PriceBarRow>> barsBySymbol,
        int timeframeMinutes)
    {
        var missedTrades = new List<MissedTrade>();

        var targetSizeBySymbol = orders
            .GroupBy(o => NormalizeSymbol(o.Symbol))
            .ToDictionary(
                g => g.Key,
                g => g.Sum(o => (o.SizeValue ?? 0m) * (o.Aum ?? 0m) * GetSideMultiplier(o.Side)),
                StringComparer.OrdinalIgnoreCase);

        var fillsBySymbol = fills
            .GroupBy(f => NormalizeSymbol(f.Symbol))
            .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.OrdinalIgnoreCase);

        foreach (var order in orders)
        {
            var normalizedSymbol = NormalizeSymbol(order.Symbol);

            if (!barsBySymbol.TryGetValue(normalizedSymbol, out var bars) || bars.Count == 0)
            {
                continue;
            }

            var scheduledUtc = order.ScheduledTimestamp.UtcDateTime;
            var barIndex = bars.FindIndex(b => b.BarTimeUtc == scheduledUtc);

            if (barIndex <= 0 || barIndex >= bars.Count)
            {
                continue;
            }

            var currentBar = bars[barIndex];
            var previousBar = bars[barIndex - 1];

            if (currentBar.Close == 0m || previousBar.Close == 0m)
            {
                continue;
            }

            var barStart = currentBar.BarTimeUtc;
            var barEnd = barStart.AddMinutes(timeframeMinutes);

            var barFills = fillsBySymbol.TryGetValue(normalizedSymbol, out var symbolFills)
                ? symbolFills.Where(f => f.ExecuteTimestamp.UtcDateTime >= barStart && f.ExecuteTimestamp.UtcDateTime < barEnd)
                : Enumerable.Empty<FillRow>();

            var filledSize = barFills.Sum(f => (f.ExecuteSize ?? 0m) * GetSideMultiplier(f.Side));
            if (!targetSizeBySymbol.TryGetValue(normalizedSymbol, out var targetSize))
            {
                continue;
            }
            var sizeDifference = targetSize - filledSize;

            if (Math.Abs(sizeDifference) < 1_000m)
            {
                continue;
            }

            var priceDelta = currentBar.Close - previousBar.Close;
            var missedPnl = sizeDifference * priceDelta;

            missedTrades.Add(new MissedTrade(
                order.Symbol,
                currentBar.BarTimeUtc,
                targetSize,
                filledSize,
                sizeDifference,
                priceDelta,
                missedPnl));
        }

        return missedTrades;
    }

    private Dictionary<string, List<(string Target, decimal Rate)>> BuildConversionGraph(
        IReadOnlyDictionary<string, decimal> closePrices)
    {
        var graph = new Dictionary<string, List<(string Target, decimal Rate)>>(StringComparer.OrdinalIgnoreCase);

        foreach (var (symbol, price) in closePrices)
        {
            if (price == 0m)
            {
                continue;
            }

            if (!CurrencyPairParser.TryParse(symbol, out var pair))
            {
                continue;
            }

            AddEdge(graph, pair.BaseCurrency, pair.QuoteCurrency, price);
            AddEdge(graph, pair.QuoteCurrency, pair.BaseCurrency, 1m / price);
        }

        return graph;
    }

    private decimal? AggregateToUsd(
        IReadOnlyDictionary<string, decimal> pnlByCurrency,
        IReadOnlyDictionary<string, List<(string Target, decimal Rate)>> conversionGraph)
    {
        decimal total = 0m;
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
                _logger.LogWarning("Unable to convert {Currency} PnL to USD using close prices.", currency);
                continue;
            }

            total += amount * conversionRate;
            convertedAny = true;
        }

        return convertedAny ? total : null;
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

    private bool TryConvertToUsd(
        decimal amount,
        string currency,
        IReadOnlyDictionary<string, List<(string Target, decimal Rate)>> conversionGraph,
        out decimal converted)
    {
        if (string.Equals(currency, "USD", StringComparison.OrdinalIgnoreCase))
        {
            converted = amount;
            return true;
        }

        if (conversionGraph.Count == 0)
        {
            converted = 0m;
            return false;
        }

        if (TryGetConversionRate(currency, "USD", conversionGraph, out var rate) && rate != 0m)
        {
            converted = amount * rate;
            return true;
        }

        _logger.LogWarning("Unable to convert {Currency} trading cost notional to USD using close prices.", currency);
        converted = 0m;
        return false;
    }

    private sealed class RealPnlComputationResult
    {
        public RealPnlComputationResult(Dictionary<string, decimal> totals, decimal totalTradingCostUsd)
        {
            Totals = totals;
            TotalTradingCostUsd = totalTradingCostUsd;
        }

        public Dictionary<string, decimal> Totals { get; }

        public decimal TotalTradingCostUsd { get; }
    }

    private static TimeZoneInfo NewYorkTimeZone
        => TimeZoneInfo.FindSystemTimeZoneById(
            OperatingSystem.IsWindows() ? "Eastern Standard Time" : "America/New_York");

    private static DateOnly GetPreviousWeekday(DateOnly date)
    {
        var previous = date.AddDays(-1);

        while (previous.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday)
        {
            previous = previous.AddDays(-1);
        }

        return previous;
    }

    private string FormatSql(string template)
    {
        return template
            .Replace("{WakettOrder}", _orderTable)
            .Replace("{WakettFill}", _fillTable)
            .Replace("{IntradayMarketPriceBarView}", _priceBarView)
            .Replace("{TimeframeMinute}", _timeframeLiteral);
    }

    private static string NormalizeSymbolForQuery(string? symbol)
    {
        if (string.IsNullOrWhiteSpace(symbol))
        {
            return string.Empty;
        }

        var normalized = symbol.Trim().ToUpperInvariant();
        return normalized.Replace("/", string.Empty);
    }

    private static string NormalizeSymbol(string? symbol)
    {
        if (string.IsNullOrWhiteSpace(symbol))
        {
            return string.Empty;
        }

        var normalized = symbol.Trim().ToUpperInvariant();
        normalized = normalized.Replace("/", string.Empty)
            .Replace("-", string.Empty)
            .Replace("_", string.Empty)
            .Replace(" ", string.Empty);

        return normalized;
    }

    private static IReadOnlyCollection<SymbolQuery> BuildSymbolQueries(IEnumerable<string> symbols)
    {
        var queries = new Dictionary<string, SymbolQuery>(StringComparer.OrdinalIgnoreCase);

        foreach (var symbol in symbols)
        {
            if (string.IsNullOrWhiteSpace(symbol))
            {
                continue;
            }

            if (CurrencyPairParser.TryParse(symbol, out var pair))
            {
                var querySymbol = NormalizeSymbolForQuery(pair.FormattedSymbol);
                var targetSymbol = NormalizeSymbol(pair.FormattedSymbol);
                var invertPrices = false;

                if (string.Equals(pair.BaseCurrency, "USD", StringComparison.OrdinalIgnoreCase))
                {
                    querySymbol = NormalizeSymbolForQuery(pair.ReversedFormattedSymbol);
                    invertPrices = true;
                }

                queries.TryAdd(querySymbol, new SymbolQuery(querySymbol, targetSymbol, invertPrices));
            }
            else
            {
                var normalized = NormalizeSymbolForQuery(symbol);

                if (normalized.Length > 0)
                {
                    queries.TryAdd(normalized, new SymbolQuery(normalized, NormalizeSymbol(symbol), false));
                }
            }
        }

        return queries.Values;
    }

    private sealed record SymbolQuery(string QuerySymbol, string TargetSymbol, bool InvertPrice);

    private sealed record OrderRow(
        long WakettOrderId,
        string Symbol,
        string? Side,
        decimal? SizeValue,
        decimal? Aum,
        DateTimeOffset ScheduledTimestamp);

    private sealed record FillRow(
        long WakettFillId,
        string Symbol,
        string? Side,
        decimal? ExecuteSize,
        decimal? ExecutePrice,
        DateTimeOffset ExecuteTimestamp);

    private sealed record PriceBarRow(string Symbol, DateTime BarTimeUtc, decimal Close);

    private sealed record MissedTrade(
        string Symbol,
        DateTime BarTimeUtc,
        decimal TargetSize,
        decimal FilledSize,
        decimal SizeDifference,
        decimal PriceDelta,
        decimal MissedPnl);
}
