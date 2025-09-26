using System.Collections.Generic;
using System.Data;
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
        ["US"] = (TimeSpan.Parse("09:00", CultureInfo.InvariantCulture), TimeSpan.Parse("15:59", CultureInfo.InvariantCulture)),
        ["US2"] = (TimeSpan.Parse("09:00", CultureInfo.InvariantCulture), TimeSpan.Parse("13:59", CultureInfo.InvariantCulture)),
        ["EU"] = (TimeSpan.Parse("02:00", CultureInfo.InvariantCulture), TimeSpan.Parse("08:59", CultureInfo.InvariantCulture)),
        ["EUUS"] = (TimeSpan.Parse("02:00", CultureInfo.InvariantCulture), TimeSpan.Parse("11:59", CultureInfo.InvariantCulture)),
        ["AS"] = (TimeSpan.Parse("20:00", CultureInfo.InvariantCulture), TimeSpan.Parse("01:59", CultureInfo.InvariantCulture)),
        ["ASEU"] = (TimeSpan.Parse("20:00", CultureInfo.InvariantCulture), TimeSpan.Parse("08:59", CultureInfo.InvariantCulture)),
        ["ALL"] = (TimeSpan.Parse("02:00", CultureInfo.InvariantCulture), TimeSpan.Parse("15:59", CultureInfo.InvariantCulture)),
    };

    private static readonly IReadOnlyList<string> SessionPreference = new[]
    {
        "US",
        "US2",
        "EU",
        "EUUS",
        "AS",
        "ASEU",
        "ALL"
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

    public async Task SendOrdersAsync(double? aumOverride = null, CancellationToken cancellationToken = default)
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

        var barInterval = ResolveBarInterval(latest, latestBarTimeUtc);
        var session = ResolveTradingSession(latestBarTimeUtc);
        var schedule = await LoadModelScheduleAsync(connection, cancellationToken);
        var orderTimestampUtc = CalculateOrderTimestamp(
            latestBarTimeUtc,
            barInterval,
            session,
            schedule?.Offset,
            schedule?.BarSize);

        var symbolMap = await LoadSymbolMapAsync(connection, cancellationToken);
        if (symbolMap.Count == 0)
        {
            _logger.LogWarning("No Wakett symbols configured. Aborting order submission.");
            return;
        }

        var allowedSymbols = LoadAllowedSymbols();
        if (allowedSymbols.Count == 0)
        {
            _logger.LogWarning("No allowed Wakett symbols configured. Aborting order submission.");
            return;
        }

        var orders = BuildOrders(latestWeights, symbolMap, allowedSymbols);
        if (orders.Count == 0)
        {
            _logger.LogInformation("No non-zero weights available for Wakett order submission.");
            return;
        }

        var request = new WakettOrderRequest
        {
            ts = FormatTimestamp(orderTimestampUtc),
            aum = aumOverride ?? ResolveAum(),
            orders = orders
        };

        var response = await _wakettApiClient.SendOrdersAsync(request);

        if (response?.Orders is { Count: > 0 })
        {
            foreach (var item in response.Orders)
            {
                if (item.error is not null)
                {
                    _logger.LogError(
                        "Wakett rejected order for {Symbol}: {Code} {Message}.",
                        item.symbol,
                        item.error.Code,
                        item.error.Message);
                }
                else
                {
                    _logger.LogInformation(
                        "Wakett accepted order for {Symbol} with side {Side}.",
                        item.symbol,
                        item.side);
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

    internal static DateTime CalculateOrderTimestamp(
        DateTime barTimeUtc,
        TimeSpan barInterval,
        string sessionKey,
        int? offsetMinutes = null,
        int? barSizeMinutes = null)
    {
        if (!SessionBounds.TryGetValue(sessionKey, out var bounds))
        {
            bounds = SessionBounds["US"];
        }

        var zone = NewYorkZone;
        var local = TimeZoneInfo.ConvertTimeFromUtc(barTimeUtc, zone);
        var (sessionStart, sessionEnd) = GetSessionWindow(local, bounds);

        var scheduleCandidate = TryGetNextScheduledTime(
            local,
            sessionStart,
            sessionEnd,
            offsetMinutes,
            barSizeMinutes);

        if (scheduleCandidate is not null)
        {
            return TimeZoneInfo.ConvertTimeToUtc(scheduleCandidate.Value, zone);
        }

        var candidate = local + barInterval;
        if (candidate <= sessionEnd)
        {
            return TimeZoneInfo.ConvertTimeToUtc(candidate, zone);
        }

        var nextSessionStart = AlignNextSessionStart(
            sessionStart,
            barInterval,
            offsetMinutes,
            barSizeMinutes);
        return TimeZoneInfo.ConvertTimeToUtc(nextSessionStart, zone);
    }

    private static DateTime? TryGetNextScheduledTime(
        DateTime local,
        DateTime sessionStart,
        DateTime sessionEnd,
        int? offsetMinutes,
        int? barSizeMinutes)
    {
        if (offsetMinutes is not > 0)
        {
            return null;
        }

        var baseMinute = Math.Max(0, barSizeMinutes ?? 0);
        var step = offsetMinutes.Value;

        var withinSession = GetNextScheduledLocal(local, step, baseMinute, strictlyGreater: true);
        if (withinSession <= sessionEnd)
        {
            return withinSession;
        }

        var duration = sessionEnd - sessionStart;
        if (duration <= TimeSpan.Zero)
        {
            duration += TimeSpan.FromDays(1);
        }

        var nextSessionStart = sessionStart.AddDays(1);
        var nextSessionEnd = nextSessionStart + duration;

        var nextSession = GetNextScheduledLocal(nextSessionStart, step, baseMinute, strictlyGreater: false);
        if (nextSession > nextSessionEnd)
        {
            return nextSessionStart;
        }

        return nextSession;
    }

    private static DateTime AlignNextSessionStart(
        DateTime sessionStart,
        TimeSpan barInterval,
        int? offsetMinutes,
        int? barSizeMinutes)
    {
        var nextSessionStart = sessionStart.AddDays(1);

        if (offsetMinutes is > 0)
        {
            var baseMinute = Math.Max(0, barSizeMinutes ?? 0);
            return GetNextScheduledLocal(nextSessionStart, offsetMinutes.Value, baseMinute, strictlyGreater: false);
        }

        var alignment = barSizeMinutes.GetValueOrDefault((int)Math.Round(barInterval.TotalMinutes));
        if (alignment <= 0)
        {
            return nextSessionStart;
        }

        var minutesIntoDay = (int)Math.Floor(nextSessionStart.TimeOfDay.TotalMinutes);
        var remainder = minutesIntoDay % alignment;
        if (remainder == 0)
        {
            return nextSessionStart;
        }

        var delta = alignment - remainder;
        return nextSessionStart.AddMinutes(delta);
    }

    private static DateTime GetNextScheduledLocal(
        DateTime reference,
        int stepMinutes,
        int baseMinute,
        bool strictlyGreater)
    {
        const int MinutesPerDay = 24 * 60;

        var minutes = (int)Math.Floor(reference.TimeOfDay.TotalMinutes);

        int nextMinute;
        if (minutes < baseMinute || (minutes == baseMinute && !strictlyGreater))
        {
            nextMinute = baseMinute;
        }
        else
        {
            var delta = minutes - baseMinute;
            var steps = delta / stepMinutes;
            var remainder = delta % stepMinutes;

            if (remainder == 0)
            {
                if (strictlyGreater)
                {
                    steps += 1;
                }
            }
            else
            {
                steps += 1;
            }

            nextMinute = baseMinute + (steps * stepMinutes);
        }

        var dayOffset = Math.DivRem(nextMinute, MinutesPerDay, out var minuteOfDay);
        var candidateDate = reference.Date.AddDays(dayOffset);
        return candidateDate + TimeSpan.FromMinutes(minuteOfDay);
    }

    private TimeSpan ResolveBarInterval(IReadOnlyList<TheoreticalWeightRow> weights, DateTime latestBarTimeUtc)
    {
        foreach (var row in weights)
        {
            if (row.BarTimeUtc < latestBarTimeUtc)
            {
                var interval = latestBarTimeUtc - row.BarTimeUtc;
                if (interval > TimeSpan.Zero)
                {
                    return interval;
                }
            }
        }

        var configured = Environment.GetEnvironmentVariable("WAKETT_BAR_INTERVAL_MINUTES")
            ?? _configuration["ExternalApis:WakettApi:BarIntervalMinutes"];

        if (int.TryParse(configured, NumberStyles.Integer, CultureInfo.InvariantCulture, out var minutes) && minutes > 0)
        {
            return TimeSpan.FromMinutes(minutes);
        }

        var programme = _configuration.GetSection("Programmes").GetChildren()
            .FirstOrDefault(section => int.TryParse(section["ModelId"], out var id) && id == TargetModelId);

        if (programme is not null && int.TryParse(programme["Timeframe"], NumberStyles.Integer, CultureInfo.InvariantCulture, out var programmeMinutes) && programmeMinutes > 0)
        {
            return TimeSpan.FromMinutes(programmeMinutes);
        }

        return TimeSpan.FromHours(1);
    }

    private string ResolveTradingSession(DateTime barTimeUtc)
    {
        var configured = Environment.GetEnvironmentVariable("WAKETT_SESSION")
            ?? _configuration["ExternalApis:WakettApi:Session"];

        if (!string.IsNullOrWhiteSpace(configured))
        {
            var normalized = configured.Trim().ToUpperInvariant();
            if (SessionBounds.ContainsKey(normalized))
            {
                return normalized;
            }
        }

        var programme = _configuration.GetSection("Programmes").GetChildren()
            .FirstOrDefault(section => int.TryParse(section["ModelId"], out var id) && id == TargetModelId);

        if (programme is not null)
        {
            var programmeSession = programme["Session"];
            if (!string.IsNullOrWhiteSpace(programmeSession))
            {
                var normalized = programmeSession.Trim().ToUpperInvariant();
                if (SessionBounds.ContainsKey(normalized))
                {
                    return normalized;
                }
            }
        }

        var local = TimeZoneInfo.ConvertTimeFromUtc(barTimeUtc, NewYorkZone);
        foreach (var session in SessionPreference)
        {
            if (SessionBounds.TryGetValue(session, out var bounds) && IsWithinSession(local, bounds))
            {
                return session;
            }
        }

        return "US";
    }

    private static (DateTime Start, DateTime End) GetSessionWindow(DateTime local, (TimeSpan Start, TimeSpan End) bounds)
    {
        var duration = bounds.End - bounds.Start;
        if (duration <= TimeSpan.Zero)
        {
            duration += TimeSpan.FromDays(1);
        }

        var start = local.Date + bounds.Start;

        while (true)
        {
            var end = start + duration;
            if (local >= start && local <= end)
            {
                return (start, end);
            }

            start = local < start ? start.AddDays(-1) : start.AddDays(1);
        }
    }

    private static bool IsWithinSession(DateTime local, (TimeSpan Start, TimeSpan End) bounds)
    {
        var time = local.TimeOfDay;
        if (bounds.Start <= bounds.End)
        {
            return time >= bounds.Start && time <= bounds.End;
        }

        return time >= bounds.Start || time <= bounds.End;
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

    protected virtual async Task<ModelScheduleRow?> LoadModelScheduleAsync(
        IDbConnection connection,
        CancellationToken cancellationToken)
    {
        const string sql = @"SELECT TOP (1)
    BarSize,
    Offset
FROM [Intraday].[model].[Model]
WHERE ModelId = @ModelId
ORDER BY ModelId";

        var definition = new CommandDefinition(
            sql,
            new { ModelId = TargetModelId },
            cancellationToken: cancellationToken);

        var row = await connection.QueryFirstOrDefaultAsync<ModelScheduleRow>(definition);
        return row;
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

    protected virtual async Task<Dictionary<int, string>> LoadSymbolMapAsync(
        IDbConnection connection,
        CancellationToken cancellationToken)
    {
        const string sql = @"SELECT SecurityId, Symbol
FROM [Intraday].[core].[Security]
WHERE IsActive = 1 AND Symbol IS NOT NULL AND LTRIM(RTRIM(Symbol)) <> ''";

        var definition = new CommandDefinition(sql, cancellationToken: cancellationToken);
        var rows = await connection.QueryAsync<SecuritySymbolRow>(definition);

        var map = new Dictionary<int, string>();
        foreach (var row in rows)
        {
            if (string.IsNullOrWhiteSpace(row.Symbol))
            {
                continue;
            }

            var sanitized = row.Symbol.Trim().Replace("/", string.Empty).ToUpperInvariant();

            if (sanitized.Length != 6)
            {
                continue;
            }

            map[row.SecurityId] = sanitized;
        }

        return map;
    }

    private HashSet<string> LoadAllowedSymbols()
    {
        var allowed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var item in EnumerateConfiguredSymbols("ExternalApis:WakettApi:Symbols"))
        {
            allowed.Add(item);
        }

        foreach (var item in EnumerateConfiguredSymbols("ExternalApis:WakettApi:MissingSymbols"))
        {
            allowed.Add(item);
        }

        return allowed;
    }

    private IEnumerable<string> EnumerateConfiguredSymbols(string section)
    {
        var configured = _configuration
            .GetSection(section)
            .Get<List<WakettSecuritySymbol>>() ?? new();

        foreach (var symbol in configured)
        {
            if (SymbolInfo.TryCreate(symbol.SecurityId, symbol.Symbol, out var info))
            {
                yield return info.FormattedSymbol;
            }
        }
    }

    private static List<WakettOrderItem> BuildOrders(
        IEnumerable<TheoreticalWeightRow> weights,
        IReadOnlyDictionary<int, string> symbolMap,
        ISet<string> allowedSymbols)
    {
        var parsedSymbols = symbolMap
            .Select(pair => SymbolInfo.TryCreate(pair.Key, pair.Value, out var info) ? info : null)
            .Where(info => info is not null)
            .Cast<SymbolInfo>()
            .ToDictionary(info => info.SecurityId);

        if (parsedSymbols.Count == 0)
        {
            return new List<WakettOrderItem>();
        }

        var exposures = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);

        foreach (var weight in weights)
        {
            if (weight.Weight == 0m)
            {
                continue;
            }

            if (!parsedSymbols.TryGetValue(weight.SecurityId, out var symbol))
            {
                continue;
            }

            AddExposure(exposures, symbol.BaseCurrency, weight.Weight);
            AddExposure(exposures, symbol.QuoteCurrency, -weight.Weight);
        }

        if (exposures.Count == 0)
        {
            return new List<WakettOrderItem>();
        }

        var usdBasePairs = parsedSymbols.Values
            .Where(info => string.Equals(info.QuoteCurrency, "USD", StringComparison.OrdinalIgnoreCase))
            .GroupBy(info => info.BaseCurrency, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(
                group => group.Key,
                group => group.OrderBy(info => info.SecurityId).First(),
                StringComparer.OrdinalIgnoreCase);

        var usdQuotePairs = parsedSymbols.Values
            .Where(info => string.Equals(info.BaseCurrency, "USD", StringComparison.OrdinalIgnoreCase))
            .GroupBy(info => info.QuoteCurrency, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(
                group => group.Key,
                group => group.OrderBy(info => info.SecurityId).First(),
                StringComparer.OrdinalIgnoreCase);

        var orderDescriptors = new List<(int SecurityId, SymbolInfo Info, decimal Weight)>();

        foreach (var kvp in exposures)
        {
            var currency = kvp.Key;
            var exposure = kvp.Value;

            if (string.Equals(currency, "USD", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (exposure == 0m)
            {
                continue;
            }

            if (usdBasePairs.TryGetValue(currency, out var basePair))
            {
                orderDescriptors.Add((basePair.SecurityId, basePair, exposure));
                continue;
            }

            if (usdQuotePairs.TryGetValue(currency, out var quotePair))
            {
                orderDescriptors.Add((quotePair.SecurityId, quotePair, -exposure));
            }
        }

        var result = new List<WakettOrderItem>();

        foreach (var order in orderDescriptors.Where(order => order.Weight != 0m).OrderBy(order => order.SecurityId))
        {
            var formatted = order.Info.FormattedSymbol;
            var side = order.Weight > 0 ? "BUY" : "SELL";
            var value = Math.Abs((double)order.Weight);

            if (!allowedSymbols.Contains(formatted))
            {
                var reversed = order.Info.ReversedFormattedSymbol;
                if (allowedSymbols.Contains(reversed))
                {
                    formatted = reversed;
                    side = side == "BUY" ? "SELL" : "BUY";
                }
                else
                {
                    continue;
                }
            }

            result.Add(new WakettOrderItem
            {
                symbol = formatted,
                side = side,
                code = $"QQB-{order.SecurityId}",
                size = new WakettOrderSize
                {
                    type = "percentage",
                    value = value
                }
            });
        }

        return result;
    }

    private static void AddExposure(IDictionary<string, decimal> exposures, string currency, decimal value)
    {
        if (value == 0m)
        {
            return;
        }

        if (!exposures.TryGetValue(currency, out var existing))
        {
            exposures[currency] = value;
            return;
        }

        var updated = existing + value;

        if (updated == 0m)
        {
            exposures.Remove(currency);
        }
        else
        {
            exposures[currency] = updated;
        }
    }

    private sealed record SymbolInfo(int SecurityId, string Symbol, string BaseCurrency, string QuoteCurrency)
    {
        public string FormattedSymbol => $"{BaseCurrency}/{QuoteCurrency}";
        public string ReversedFormattedSymbol => $"{QuoteCurrency}/{BaseCurrency}";

        public static bool TryCreate(int securityId, string? symbol, out SymbolInfo? info)
        {
            info = null;

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

            info = new SymbolInfo(securityId, trimmed, baseCurrency, quoteCurrency);
            return true;
        }
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

    protected sealed record TheoreticalWeightRow
    {
        public int SecurityId { get; init; }

        public int ModelId { get; init; }

        public DateTime BarTimeUtc { get; init; }

        public long ModelRunId { get; init; }

        public decimal Weight { get; init; }
    }

    private sealed record SecuritySymbolRow
    {
        public int SecurityId { get; init; }

        public string? Symbol { get; init; }
    }

    protected sealed record ModelScheduleRow
    {
        public int? BarSize { get; init; }

        public int? Offset { get; init; }
    }
}
