using System.Globalization;
using System.Linq;
using System.Runtime.InteropServices;
using Dapper;
using Microsoft.Extensions.Configuration;
using TradingDaemon.Data;
using TradingDaemon.Models;

namespace TradingDaemon.Services;

public class OrderSender
{
    private static readonly IReadOnlyDictionary<string, (TimeSpan Start, TimeSpan End)> SessionBounds = new Dictionary<string, (TimeSpan Start, TimeSpan End)>
    {
        ["US"] = (TimeSpan.Parse("09:30", CultureInfo.InvariantCulture), TimeSpan.Parse("15:59", CultureInfo.InvariantCulture)),
        ["US2"] = (TimeSpan.Parse("09:00", CultureInfo.InvariantCulture), TimeSpan.Parse("13:59", CultureInfo.InvariantCulture)),
        ["EU"] = (TimeSpan.Parse("02:00", CultureInfo.InvariantCulture), TimeSpan.Parse("08:59", CultureInfo.InvariantCulture)),
        ["EUUS"] = (TimeSpan.Parse("02:00", CultureInfo.InvariantCulture), TimeSpan.Parse("11:59", CultureInfo.InvariantCulture)),
        ["AS"] = (TimeSpan.Parse("20:00", CultureInfo.InvariantCulture), TimeSpan.Parse("01:59", CultureInfo.InvariantCulture)),
        ["ASEU"] = (TimeSpan.Parse("20:00", CultureInfo.InvariantCulture), TimeSpan.Parse("08:59", CultureInfo.InvariantCulture)),
        ["ALL"] = (TimeSpan.Parse("02:00", CultureInfo.InvariantCulture), TimeSpan.Parse("15:59", CultureInfo.InvariantCulture)),
    };

    private const int TargetModelId = 1;

    private readonly WakettApiClient _wakettApiClient;
    private readonly DapperContext _context;
    private readonly ILogger<OrderSender> _logger;
    private readonly IConfiguration _configuration;
    private readonly TimeProvider _timeProvider;

    public OrderSender(
        WakettApiClient wakettApiClient,
        DapperContext context,
        ILogger<OrderSender> logger,
        IConfiguration configuration,
        TimeProvider? timeProvider = null)
    {
        _wakettApiClient = wakettApiClient;
        _context = context;
        _logger = logger;
        _configuration = configuration;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task SendOrdersAsync(CancellationToken cancellationToken = default)
    {
        using var connection = _context.CreateConnection();
        var latest = await LoadLatestWeightsAsync(connection, cancellationToken);
        if (latest.Count == 0)
        {
            _logger.LogWarning("No theoretical weights found for model {ModelId}.", TargetModelId);
            return;
        }

        var latestBarTimeUtc = latest[0].BarTimeUtc;
        var utcNow = _timeProvider.GetUtcNow().UtcDateTime;

        if (!IsBarRecentEnough(latestBarTimeUtc, utcNow))
        {
            return;
        }

        var latestWeights = latest.Where(w => w.BarTimeUtc == latestBarTimeUtc).ToList();

        var symbolMap = LoadSymbolMap();
        if (symbolMap.Count == 0)
        {
            _logger.LogWarning("No Wakett symbols configured. Aborting order submission.");
            return;
        }

        var orders = BuildOrders(latestWeights, symbolMap);
        if (orders.Count == 0)
        {
            _logger.LogInformation("No non-zero weights available for Wakett order submission.");
            return;
        }

        var request = new WakettOrderRequest
        {
            Ts = FormatTimestamp(latestBarTimeUtc),
            Aum = ResolveAum(),
            Orders = orders
        };

        var response = await _wakettApiClient.SendOrdersAsync(request);

        if (response?.Orders is { Count: > 0 })
        {
            foreach (var item in response.Orders)
            {
                if (item.Error is not null)
                {
                    _logger.LogError(
                        "Wakett rejected order for {Symbol}: {Code} {Message}.",
                        item.Symbol,
                        item.Error.Code,
                        item.Error.Message);
                }
                else
                {
                    _logger.LogInformation(
                        "Wakett accepted order for {Symbol} with side {Side}.",
                        item.Symbol,
                        item.Side);
                }
            }
        }
    }

    internal static string FormatTimestamp(DateTime barTimeUtc)
    {
        var local = TimeZoneInfo.ConvertTimeFromUtc(barTimeUtc, NewYorkZone);
        var offset = NewYorkZone.GetUtcOffset(local);
        var sign = offset < TimeSpan.Zero ? "-" : "+";
        var abs = offset.Duration();
        var offsetString = $"{sign}{abs.Hours:00}{abs.Minutes:00}";
        return $"{local:yyyy-MM-dd HH:mm:ss.fff}{offsetString}";
    }

    protected virtual async Task<IReadOnlyList<TheoreticalWeightRow>> LoadLatestWeightsAsync(
        IDbConnection connection,
        CancellationToken cancellationToken)
    {
        const string sql = @"SELECT TOP (1000)
    tw.SecurityId,
    tw.ModelId,
    tw.BarTimeUtc,
    tw.ModelRunId,
    tw.Weight
FROM [Intraday].[model].[TheoreticalWeight] tw
WHERE tw.ModelId = @ModelId
ORDER BY tw.BarTimeUtc DESC, tw.SecurityId";

        var definition = new CommandDefinition(
            sql,
            new { ModelId = TargetModelId },
            cancellationToken: cancellationToken);

        var rows = await connection.QueryAsync<TheoreticalWeightRow>(definition);
        return rows.ToList();
    }

    private bool IsBarRecentEnough(DateTime barTimeUtc, DateTime utcNow)
    {
        var zone = CentralEuropeZone;
        var barLocal = TimeZoneInfo.ConvertTimeFromUtc(barTimeUtc, zone);
        var nowLocal = TimeZoneInfo.ConvertTimeFromUtc(utcNow, zone);

        if (barLocal.Date < nowLocal.Date)
        {
            _logger.LogInformation(
                "Using previous day's theoretical weights from {BarTimeUtc:O} for first trade of the day.",
                barTimeUtc);
            return true;
        }

        if (utcNow - barTimeUtc > TimeSpan.FromMinutes(60))
        {
            _logger.LogWarning(
                "Latest theoretical weights are stale. Last bar: {BarTimeUtc:O}, now: {NowUtc:O}.",
                barTimeUtc,
                utcNow);
            return false;
        }

        return true;
    }

    private Dictionary<int, string> LoadSymbolMap()
    {
        var configured = _configuration
            .GetSection("ExternalApis:WakettApi:Symbols")
            .Get<List<WakettSecuritySymbol>>() ?? new List<WakettSecuritySymbol>();

        var map = new Dictionary<int, string>();
        foreach (var symbol in configured)
        {
            map[symbol.SecurityId] = symbol.Symbol;
        }

        return map;
    }

    private static List<WakettOrderItem> BuildOrders(
        IEnumerable<TheoreticalWeightRow> weights,
        IReadOnlyDictionary<int, string> symbolMap)
    {
        var orders = new List<WakettOrderItem>();

        foreach (var weight in weights.OrderBy(w => w.SecurityId))
        {
            if (!symbolMap.TryGetValue(weight.SecurityId, out var symbol))
            {
                continue;
            }

            if (weight.Weight == 0m)
            {
                continue;
            }

            var side = weight.Weight > 0 ? "BUY" : "SELL";
            var sizeValue = Math.Abs((double)weight.Weight);

            orders.Add(new WakettOrderItem
            {
                Symbol = symbol,
                Side = side,
                Code = $"QQB-{weight.SecurityId}",
                Size = new WakettOrderSize
                {
                    Type = "percentage",
                    Value = sizeValue
                }
            });
        }

        return orders;
    }

    private double? ResolveAum()
    {
        var configured = Environment.GetEnvironmentVariable("WAKETT_AUM")
            ?? _configuration["ExternalApis:WakettApi:Aum"];

        if (string.IsNullOrWhiteSpace(configured))
        {
            return null;
        }

        if (double.TryParse(configured, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed;
        }

        _logger.LogWarning("Unable to parse Wakett AUM value '{Value}'.", configured);
        return null;
    }

    private static TimeZoneInfo NewYorkZone => TimeZoneInfo.FindSystemTimeZoneById(
        RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Eastern Standard Time" : "America/New_York");

    private static TimeZoneInfo CentralEuropeZone => TimeZoneInfo.FindSystemTimeZoneById(
        RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Central European Standard Time" : "Europe/Berlin");

    internal sealed record TheoreticalWeightRow
    {
        public int SecurityId { get; init; }

        public int ModelId { get; init; }

        public DateTime BarTimeUtc { get; init; }

        public long ModelRunId { get; init; }

        public decimal Weight { get; init; }
    }
}
