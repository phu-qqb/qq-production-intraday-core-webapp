using System.Globalization;
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
    TradeTimestamp
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

    public SlippageAndMissedCostService(
        DapperContext context,
        ILogger<SlippageAndMissedCostService> logger,
        IDatabaseObjectNameProvider databaseObjectNameProvider,
        IOptions<PriceBarOptions>? priceBarOptions = null)
    {
        _context = context;
        _logger = logger;
        var options = priceBarOptions?.Value ?? new PriceBarOptions();
        _timeframeLiteral = Math.Max(1, options.TimeframeMinute).ToString(CultureInfo.InvariantCulture);
        _orderTable = databaseObjectNameProvider.GetObjectName(DatabaseObjects.WakettOrder);
        _fillTable = databaseObjectNameProvider.GetObjectName(DatabaseObjects.WakettFill);
        _priceBarView = databaseObjectNameProvider.GetObjectName(DatabaseObjects.IntradayMarketPriceBarView);
    }

    public async Task<SlippageResult> ComputeAsync(SlippageRequest request, CancellationToken cancellationToken)
    {
        var tradingDate = DateOnly.FromDateTime(request.Date.Date);
        var startUtc = DateTime.SpecifyKind(tradingDate.ToDateTime(TimeOnly.MinValue), DateTimeKind.Utc);
        var endUtc = startUtc.AddDays(1);

        using var connection = _context.CreateConnection();

        var ordersTask = connection.QueryAsync<OrderRow>(
            new CommandDefinition(FormatSql(OrdersSqlTemplate), new { StartUtc = startUtc, EndUtc = endUtc }, cancellationToken: cancellationToken));

        var fillsTask = connection.QueryAsync<FillRow>(
            new CommandDefinition(FormatSql(FillsSqlTemplate), new { StartUtc = startUtc, EndUtc = endUtc }, cancellationToken: cancellationToken));

        await Task.WhenAll(ordersTask, fillsTask);

        var orders = (await ordersTask).Where(o => !string.IsNullOrWhiteSpace(o.Symbol)).ToList();
        var fills = (await fillsTask).Where(f => !string.IsNullOrWhiteSpace(f.Symbol)).ToList();

        var symbolSet = orders.Select(o => NormalizeSymbolForQuery(o.Symbol))
            .Concat(fills.Select(f => NormalizeSymbolForQuery(f.Symbol)))
            .Where(s => s.Length > 0)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (symbolSet.Length == 0)
        {
            _logger.LogWarning("No symbols found for slippage computation on {Date}", tradingDate);
            return new SlippageResult(tradingDate, 0m, 0m, 0m);
        }

        var priceBarsDefinition = new CommandDefinition(
            FormatSql(PriceBarsSqlTemplate),
            new { StartUtc = startUtc, EndUtc = endUtc, Symbols = symbolSet },
            cancellationToken: cancellationToken);

        var priceBars = await connection.QueryAsync<PriceBarRow>(priceBarsDefinition);
        var barsBySymbol = priceBars
            .GroupBy(b => NormalizeSymbol(b.Symbol))
            .ToDictionary(g => g.Key, g => g.OrderBy(b => b.BarTimeUtc).ToList(), StringComparer.OrdinalIgnoreCase);

        var theoreticalUsd = CalculateTheoreticalPnlUsd(orders, barsBySymbol);
        var realUsd = CalculateRealPnlUsd(fills, barsBySymbol);
        var slippageCost = theoreticalUsd - realUsd;

        Console.WriteLine($"Slippage computation for {tradingDate:yyyy-MM-dd}");
        Console.WriteLine($"Theoretical PnL (USD): {theoreticalUsd}");
        Console.WriteLine($"Real PnL (USD): {realUsd}");
        Console.WriteLine($"Slippage and missed trade cost (USD): {slippageCost}");

        return new SlippageResult(tradingDate, theoreticalUsd, realUsd, slippageCost);
    }

    private decimal CalculateTheoreticalPnlUsd(
        IReadOnlyCollection<OrderRow> orders,
        IReadOnlyDictionary<string, List<PriceBarRow>> barsBySymbol)
    {
        decimal total = 0m;

        foreach (var order in orders)
        {
            if (!CurrencyPairParser.TryParse(order.Symbol, out var pair))
            {
                _logger.LogDebug("Skipping theoretical PnL for unparsable symbol {Symbol}", order.Symbol);
                continue;
            }

            var normalizedSymbol = NormalizeSymbol(order.Symbol);
            if (!barsBySymbol.TryGetValue(normalizedSymbol, out var bars) || bars.Count < 2)
            {
                _logger.LogDebug("No price bars for symbol {Symbol} to compute theoretical PnL", order.Symbol);
                continue;
            }

            var barIndex = bars.FindIndex(b => b.BarTimeUtc == order.ScheduledTimestamp);
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
            var lastBar = bars[^1];
            var pnlUsd = ConvertQuoteToUsd(pnlQuote, pair, lastBar.Close);

            if (pnlUsd.HasValue)
            {
                total += pnlUsd.Value;
            }
        }

        return total;
    }

    private decimal CalculateRealPnlUsd(
        IReadOnlyCollection<FillRow> fills,
        IReadOnlyDictionary<string, List<PriceBarRow>> barsBySymbol)
    {
        decimal total = 0m;
        var fillsBySymbol = fills
            .GroupBy(f => NormalizeSymbol(f.Symbol))
            .ToDictionary(g => g.Key, g => g.OrderBy(f => f.TradeTimestamp).ToList(), StringComparer.OrdinalIgnoreCase);

        foreach (var (symbol, bars) in barsBySymbol)
        {
            if (bars.Count < 2)
            {
                continue;
            }

            if (!CurrencyPairParser.TryParse(symbol, out var pair))
            {
                continue;
            }

            fillsBySymbol.TryGetValue(symbol, out var symbolFills);
            var position = 0m;
            var fillIndex = 0;

            for (var i = 0; i < bars.Count - 1; i++)
            {
                var currentBar = bars[i];
                while (symbolFills is not null && fillIndex < symbolFills.Count && symbolFills[fillIndex].TradeTimestamp < currentBar.BarTimeUtc)
                {
                    var fill = symbolFills[fillIndex];
                    var sideMultiplier = GetSideMultiplier(fill.Side);
                    position += sideMultiplier * (fill.ExecuteSize ?? 0m);
                    fillIndex++;
                }

                if (currentBar.Close == 0m)
                {
                    continue;
                }

                var nextBar = bars[i + 1];
                var priceReturn = (nextBar.Close - currentBar.Close) / currentBar.Close;
                var pnlQuote = position * priceReturn;
                var pnlUsd = ConvertQuoteToUsd(pnlQuote, pair, bars[^1].Close);

                if (pnlUsd.HasValue)
                {
                    total += pnlUsd.Value;
                }
            }
        }

        return total;
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

    private decimal? ConvertQuoteToUsd(decimal pnlQuote, CurrencyPair pair, decimal? conversionPrice)
    {
        if (string.Equals(pair.QuoteCurrency, "USD", StringComparison.OrdinalIgnoreCase))
        {
            return pnlQuote;
        }

        if (!conversionPrice.HasValue || conversionPrice.Value == 0m)
        {
            _logger.LogWarning("Missing conversion price for {Symbol}", pair.FormattedSymbol);
            return null;
        }

        if (string.Equals(pair.BaseCurrency, "USD", StringComparison.OrdinalIgnoreCase))
        {
            return pnlQuote / conversionPrice.Value;
        }

        _logger.LogWarning(
            "Unable to convert PnL for {Symbol} to USD because neither currency is USD.",
            pair.FormattedSymbol);
        return null;
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
        return string.IsNullOrWhiteSpace(symbol) ? string.Empty : symbol.Trim().ToUpperInvariant();
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

    private sealed record OrderRow(
        long WakettOrderId,
        string Symbol,
        string? Side,
        decimal? SizeValue,
        decimal? Aum,
        DateTime ScheduledTimestamp);

    private sealed record FillRow(
        long WakettFillId,
        string Symbol,
        string? Side,
        decimal? ExecuteSize,
        DateTime TradeTimestamp);

    private sealed record PriceBarRow(string Symbol, DateTime BarTimeUtc, decimal Close);
}
