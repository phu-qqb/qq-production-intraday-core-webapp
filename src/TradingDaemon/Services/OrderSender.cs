using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using TradingDaemon.Data;
using TradingDaemon.Models;
using TradingDaemon.Options;
using TradingDaemon.Utils;

namespace TradingDaemon.Services;

public class OrderSender
{
    private static readonly (TimeSpan Start, TimeSpan End) UsSessionBounds = (TimeSpan.Parse("09:00", CultureInfo.InvariantCulture), TimeSpan.Parse("15:59", CultureInfo.InvariantCulture));
    private static readonly (TimeSpan Start, TimeSpan End) UsBankHolidaySessionBounds = (TimeSpan.Parse("09:00", CultureInfo.InvariantCulture), TimeSpan.Parse("12:51", CultureInfo.InvariantCulture));

    private static readonly IReadOnlyDictionary<string, (TimeSpan Start, TimeSpan End)> SessionBounds = new Dictionary<string, (TimeSpan Start, TimeSpan End)>
    {
        ["US"] = UsSessionBounds,
        ["US2"] = (TimeSpan.Parse("09:00", CultureInfo.InvariantCulture), TimeSpan.Parse("13:59", CultureInfo.InvariantCulture)),
        ["EU"] = (TimeSpan.Parse("02:00", CultureInfo.InvariantCulture), TimeSpan.Parse("08:59", CultureInfo.InvariantCulture)),
        ["EUUS"] = (TimeSpan.Parse("02:00", CultureInfo.InvariantCulture), TimeSpan.Parse("11:59", CultureInfo.InvariantCulture)),
        ["AS"] = (TimeSpan.Parse("20:00", CultureInfo.InvariantCulture), TimeSpan.Parse("01:59", CultureInfo.InvariantCulture)),
        ["ASEU"] = (TimeSpan.Parse("20:00", CultureInfo.InvariantCulture), TimeSpan.Parse("08:59", CultureInfo.InvariantCulture)),
        ["ALL"] = (TimeSpan.Parse("02:00", CultureInfo.InvariantCulture), TimeSpan.Parse("15:59", CultureInfo.InvariantCulture)),
    };

    private static readonly IReadOnlyCollection<string> AllowedTradingSessions = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "EU",
        "US",
        "EUUS"
    };

    private static readonly IReadOnlyList<string> TradingSessionPreference = new[]
    {
        "EU",
        "EUUS",
        "US"
    };

    private static readonly TimeSpan NyWeightOverrideTime = new(12, 30, 0);
    private static readonly TimeSpan UsSessionEndOrderTime = new(15, 51, 0);
    private static readonly TimeSpan UsBankHolidaySessionEndOrderTime = new(12, 51, 0);
    private static readonly TimeSpan UsBankHolidayCutoffTime = new(12, 52, 0);

    private const int TargetModelId = 1;

    private static readonly JsonSerializerOptions OrderTradeJsonOptions = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    private readonly WakettApiClient _wakettApiClient;
    private readonly DapperContext _context;
    private readonly ILogger<OrderSender> _logger;
    private readonly IConfiguration _configuration;
    private readonly TimeProvider _timeProvider;
    private readonly string _nettedWeightTable;
    private readonly string _modelTable;
    private readonly string _securityTable;
    private readonly string _tradingLimitTable;
    private readonly string _orderTable;
    private readonly string _tradingLimitBreachTable;
    private readonly PriceBarOptions _priceBarOptions;
    private readonly bool _useUsHolidaySchedule;

    public OrderSender(
        WakettApiClient wakettApiClient,
        DapperContext context,
        ILogger<OrderSender> logger,
        IConfiguration configuration,
        IDatabaseObjectNameProvider databaseNameProvider,
        TimeProvider? timeProvider = null,
        IOptions<PriceBarOptions>? priceBarOptions = null,
        IOptions<TradingOptions>? tradingOptions = null)
    {
        _wakettApiClient = wakettApiClient;
        _context = context;
        _logger = logger;
        _configuration = configuration;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _priceBarOptions = priceBarOptions?.Value ?? new PriceBarOptions();
        _useUsHolidaySchedule = tradingOptions?.Value?.UsBankHoliday ?? false;
        _nettedWeightTable = databaseNameProvider.GetObjectName(DatabaseObjects.IntradayModelNettedWeight);
        _modelTable = databaseNameProvider.GetObjectName(DatabaseObjects.IntradayModel);
        _securityTable = databaseNameProvider.GetObjectName(DatabaseObjects.IntradayCoreSecurity);
        _tradingLimitTable = databaseNameProvider.GetObjectName(DatabaseObjects.WakettTradingLimit);
        _orderTable = databaseNameProvider.GetObjectName(DatabaseObjects.WakettOrder);
        _tradingLimitBreachTable = databaseNameProvider.GetObjectName(DatabaseObjects.WakettTradingLimitBreachReport);
    }

    public async Task SendOrdersAsync(double? aumOverride = null, CancellationToken cancellationToken = default)
    {
        using var connection = _context.CreateConnection();
        var modelDefinitions = LoadModelDefinitions();
        var modelIds = modelDefinitions.Keys.ToArray();

        var latest = await LoadLatestNettedWeightsAsync(connection, modelIds, cancellationToken);
        if (latest.Count == 0)
        {
            _logger.LogWarning("No netted weights found for models {ModelIds}.", string.Join(", ", modelIds));
            return;
        }

        var utcNow = _timeProvider.GetUtcNow().UtcDateTime;

        if (_useUsHolidaySchedule && IsPastUsHolidayCutoff(utcNow))
        {
            _logger.LogInformation(
                "US bank holiday schedule active and local time {LocalTime:HH:mm} past cutoff {Cutoff:hh\:mm}. Skipping order submission.",
                TimeZoneInfo.ConvertTimeFromUtc(utcNow, NewYorkZone),
                UsBankHolidayCutoffTime);
            return;
        }

        var modelSnapshots = BuildModelSnapshots(latest, modelDefinitions, utcNow);
        if (modelSnapshots.Count == 0)
        {
            _logger.LogWarning("No recent netted weights available for configured models {ModelIds}.", string.Join(", ", modelIds));
            return;
        }

        var orderTimestampUtc = await ResolveOrderTimestampAsync(connection, modelSnapshots, utcNow, cancellationToken);

        var symbolMap = await LoadSymbolMapAsync(connection, cancellationToken);
        if (symbolMap.Count == 0)
        {
            _logger.LogWarning("No Wakett symbols configured. Aborting order submission.");
            return;
        }

        var eligibleSnapshots = FilterSnapshotsWithinSession(modelSnapshots, orderTimestampUtc);
        if (eligibleSnapshots.Count == 0)
        {
            _logger.LogWarning(
                "No model snapshots align with order time {OrderTimestampUtc:O}. Aborting order submission.",
                orderTimestampUtc);
            return;
        }

        var aggregatedWeights = AggregateModelWeights(eligibleSnapshots, orderTimestampUtc);
        var basePairs = LoadConfiguredBasePairs();
        if (basePairs.Count == 0)
        {
            _logger.LogWarning("No Wakett base pairs configured. Aborting order submission.");
            return;
        }

        var isEndOfDayOrder = IsEndOfDayOrder(orderTimestampUtc);
        var forceHolidayFlatOrders = _useUsHolidaySchedule;
        if (isEndOfDayOrder || forceHolidayFlatOrders)
        {
            _logger.LogInformation(
                "{Context} order detected at {OrderTimestampUtc:O}. Submitting flat weights for configured base pairs.",
                _useUsHolidaySchedule ? "US bank holiday" : "End-of-day",
                orderTimestampUtc);
        }

        var builtOrders = isEndOfDayOrder || forceHolidayFlatOrders
            ? BuildEndOfDayOrders(symbolMap, basePairs, orderTimestampUtc)
            : BuildOrders(aggregatedWeights, symbolMap, orderTimestampUtc, basePairs);
        var scheduledTimestamp = ResolveScheduledTimestamp(orderTimestampUtc);
        var existingOrderSymbols = await LoadExistingOrderSymbolsAsync(
            connection,
            scheduledTimestamp,
            builtOrders,
            cancellationToken);

        if (existingOrderSymbols.Count > 0)
        {
            _logger.LogInformation(
                "Skipping Wakett order submission for scheduled timestamp {ScheduledTimestamp} because existing orders were found for symbol(s): {Symbols}.",
                scheduledTimestamp,
                string.Join(", ", existingOrderSymbols));
            return;
        }

        var aum = aumOverride ?? ResolveAum();

        var pairUsdLimits = LoadPairUsdLimits();
        if (pairUsdLimits.Count > 0 && aum is double resolvedAum && resolvedAum > 0d)
        {
            builtOrders = ApplyPairUsdLimits(builtOrders, pairUsdLimits, resolvedAum);
        }

        var isFlatOrderRequest = builtOrders.Count == 0
            || builtOrders.All(order => order.Order.size?.value == 0d);
        if (isFlatOrderRequest)
        {
            _logger.LogInformation("All Wakett order weights are zero. Submitting flat order request.");
        }

        var tradingLimits = await LoadTradingLimitsAsync(connection, cancellationToken);
        if (tradingLimits is not null)
        {
            var breaches = EvaluateTradingLimitBreaches(tradingLimits, builtOrders, aum);
            if (breaches.Count > 0)
            {
                foreach (var breach in breaches)
                {
                    _logger.LogWarning(
                        "Trading limit {LimitType} breached: observed {ObservedValue} vs limit {LimitValue}. {Details}",
                        breach.LimitType,
                        breach.ObservedValue,
                        breach.LimitValue,
                        breach.Message);
                }

                await LogTradingLimitBreachesAsync(
                    connection,
                    breaches,
                    builtOrders,
                    aum,
                    cancellationToken);
                return;
            }
        }

        var orders = builtOrders.Select(order => order.Order).ToList();

        var request = new WakettOrderRequest
        {
            ts = FormatTimestamp(orderTimestampUtc),
            aum = aum,
            execution = isFlatOrderRequest ? "EOF" : "EOC",
            orders = orders
        };

        var orderCodeLookup = builtOrders
            .Where(order => !string.IsNullOrWhiteSpace(order.Order.symbol))
            .ToDictionary(
                order => order.Order.symbol!.Trim().ToUpperInvariant(),
                order => order.Order.code,
                StringComparer.OrdinalIgnoreCase);

        var submissionTimeUtc = _timeProvider.GetUtcNow().UtcDateTime;
        var response = await _wakettApiClient.SendOrdersAsync(request);
        var receivedAtUtc = _timeProvider.GetUtcNow().UtcDateTime;

        if (response?.Orders is { Count: > 0 })
        {
            foreach (var item in response.Orders)
            {
                var resolvedOrderCode = ResolveOrderCode(item.Code, item.Symbol, orderCodeLookup) ?? string.Empty;

                if (item.Error is not null)
                {
                    _logger.LogError(
                        "Wakett rejected order {OrderCode} for {Symbol}: {Code} {Message}.",
                        resolvedOrderCode,
                        item.Symbol,
                        item.Error.Code,
                        item.Error.Message);
                }
                else
                {
                    _logger.LogInformation(
                        "Wakett accepted order {OrderCode} for {Symbol} with side {Side} and {TradeCount} trade(s).",
                        resolvedOrderCode,
                        item.Symbol,
                        item.Side,
                        item.Trades?.Count ?? 0);
                }
            }
        }
        else if (response is null)
        {
            _logger.LogWarning("Wakett order submission returned no response body.");
        }

        await PersistOrderResponseAsync(
            connection,
            request,
            response,
            orderCodeLookup,
            orderTimestampUtc,
            submissionTimeUtc,
            receivedAtUtc,
            cancellationToken);
    }

    internal static string BuildOrderCode(int securityId, DateTime orderTimestampUtc)
    {
        var utc = DateTime.SpecifyKind(orderTimestampUtc, DateTimeKind.Utc);
        var local = TimeZoneInfo.ConvertTimeFromUtc(utc, NewYorkZone);
        return $"QQB-{securityId}-{local:yyyyMMddHHmm}";
    }

    private IReadOnlyDictionary<int, ModelDefinition> LoadModelDefinitions()
    {
        var definitions = new Dictionary<int, ModelDefinition>();
        var defaultTimeframe = Math.Max(1, _priceBarOptions.TimeframeMinute);

        foreach (var model in _configuration.GetSection("Programmes").GetChildren())
        {
            if (!int.TryParse(model["ModelId"], out var modelId) || modelId <= 0)
            {
                continue;
            }

            int? timeframeMinutes = null;
            if (int.TryParse(model["Timeframe"], NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedTimeframe)
                && parsedTimeframe > 0)
            {
                timeframeMinutes = parsedTimeframe;
            }

            var session = model["Session"];

            definitions[modelId] = new ModelDefinition(modelId, timeframeMinutes ?? defaultTimeframe, session);
        }

        if (definitions.Count == 0)
        {
            definitions[TargetModelId] = new ModelDefinition(TargetModelId, defaultTimeframe, null);
        }

        return definitions;
    }

    private List<ModelSnapshot> BuildModelSnapshots(
        IReadOnlyList<NettedWeightRow> weights,
        IReadOnlyDictionary<int, ModelDefinition> modelDefinitions,
        DateTime utcNow)
    {
        var snapshots = new List<ModelSnapshot>();

        foreach (var group in weights.GroupBy(w => w.ModelId))
        {
            if (!group.Any())
            {
                continue;
            }

            var definition = modelDefinitions.TryGetValue(group.Key, out var modelDefinition)
                ? modelDefinition
                : new ModelDefinition(group.Key, null, null);

            var ordered = group.OrderByDescending(w => w.BarTimeUtc).ToList();
            var latestBarTimeUtc = ordered[0].BarTimeUtc;
            var barInterval = ResolveBarInterval(ordered, latestBarTimeUtc, definition.TimeframeMinutes);

            if (!IsBarRecentEnough(latestBarTimeUtc, utcNow, barInterval, definition.Session, group.Key))
            {
                continue;
            }

            var latestWeights = ordered.Where(w => w.BarTimeUtc == latestBarTimeUtc).ToList();
            snapshots.Add(new ModelSnapshot(group.Key, definition.Session, definition.TimeframeMinutes, barInterval, latestBarTimeUtc, latestWeights));
        }

        return snapshots;
    }

    private IReadOnlyList<ModelSnapshot> FilterSnapshotsWithinSession(
        IReadOnlyList<ModelSnapshot> snapshots,
        DateTime orderTimestampUtc)
    {
        if (snapshots.Count == 0)
        {
            return Array.Empty<ModelSnapshot>();
        }

        var orderTimeLocal = TimeZoneInfo.ConvertTimeFromUtc(orderTimestampUtc, NewYorkZone);
        var filtered = new List<ModelSnapshot>();

        foreach (var snapshot in snapshots)
        {
            if (string.IsNullOrWhiteSpace(snapshot.SessionKey))
            {
                filtered.Add(snapshot);
                continue;
            }

            var normalizedSession = snapshot.SessionKey.Trim().ToUpperInvariant();
            if (!TryResolveSessionBounds(normalizedSession, out var bounds))
            {
                _logger.LogWarning(
                    "Skipping model {ModelId} because configured session {Session} is not recognized for order time {OrderTimeLocal:O}.",
                    snapshot.ModelId,
                    snapshot.SessionKey,
                    orderTimeLocal);
                continue;
            }

            if (!IsWithinSession(orderTimeLocal, bounds))
            {
                _logger.LogInformation(
                    "Skipping model {ModelId} because order time {OrderTimeLocal:O} is outside configured session {Session}.",
                    snapshot.ModelId,
                    orderTimeLocal,
                    normalizedSession);
                continue;
            }

            filtered.Add(snapshot);
        }

        return filtered;
    }

    private static IReadOnlyList<NettedWeightRow> AggregateModelWeights(
        IReadOnlyList<ModelSnapshot> snapshots,
        DateTime orderTimestampUtc)
    {
        var aggregated = new Dictionary<int, decimal>();

        foreach (var snapshot in snapshots)
        {
            foreach (var weight in snapshot.LatestWeights)
            {
                aggregated.TryGetValue(weight.SecurityId, out var existing);
                aggregated[weight.SecurityId] = existing + weight.Weight;
            }
        }

        return aggregated
            .OrderBy(kvp => kvp.Key)
            .Select(kvp => new NettedWeightRow
            {
                SecurityId = kvp.Key,
                ModelId = TargetModelId,
                BarTimeUtc = orderTimestampUtc,
                ModelRunId = 0,
                Weight = kvp.Value
            })
            .ToList();
    }

    private Task<DateTime> ResolveOrderTimestampAsync(
        IDbConnection connection,
        IReadOnlyList<ModelSnapshot> snapshots,
        DateTime utcNow,
        CancellationToken cancellationToken)
    {
        _ = connection;
        _ = cancellationToken;

        var sessionKey = ResolveOrderSession(utcNow, snapshots);
        var normalizedSession = string.IsNullOrWhiteSpace(sessionKey)
            ? ResolveTradingSession(utcNow)
            : sessionKey;

        return Task.FromResult(CalculateOrderTimestamp(utcNow, normalizedSession, _useUsHolidaySchedule));
    }

    private string ResolveOrderSession(DateTime utcNow, IReadOnlyList<ModelSnapshot> snapshots)
    {
        var nowLocal = TimeZoneInfo.ConvertTimeFromUtc(utcNow, NewYorkZone);

        foreach (var snapshot in snapshots)
        {
            var normalized = snapshot.SessionKey?.Trim().ToUpperInvariant();
            if (string.IsNullOrWhiteSpace(normalized))
            {
                continue;
            }

            if (!TryResolveSessionBounds(normalized, out var bounds))
            {
                continue;
            }

            if (IsWithinSession(nowLocal, bounds))
            {
                return normalized;
            }
        }

        return ResolveTradingSession(utcNow);
    }

    private async Task PersistOrderResponseAsync(
        IDbConnection connection,
        WakettOrderRequest request,
        WakettOrderResponse? response,
        IReadOnlyDictionary<string, string?> orderCodeLookup,
        DateTime orderTimestampUtc,
        DateTime submissionTimeUtc,
        DateTime receivedAtUtc,
        CancellationToken cancellationToken)
    {
        if (response is not { Orders.Count: > 0 })
        {
            return;
        }

        try
        {
            var scheduledTimestamp = TryParseResponseTimestamp(response.Timestamp, out var parsedTimestamp)
                ? parsedTimestamp
                : new DateTimeOffset(DateTime.SpecifyKind(orderTimestampUtc, DateTimeKind.Utc));

            var submittedUtc = DateTime.SpecifyKind(submissionTimeUtc, DateTimeKind.Utc);
            var receivedUtc = DateTime.SpecifyKind(receivedAtUtc, DateTimeKind.Utc);
            var aumValue = request.aum.HasValue ? (decimal?)request.aum.Value : null;

            var requestLookup = new Dictionary<string, WakettOrderItem>(StringComparer.OrdinalIgnoreCase);
            foreach (var order in request.orders)
            {
                if (string.IsNullOrWhiteSpace(order.symbol))
                {
                    continue;
                }

                var key = order.symbol.Trim().ToUpperInvariant();
                requestLookup[key] = order;
            }

            var insertSql = $@"INSERT INTO {_orderTable}
(
    ModelId,
    OrderCode,
    ScheduledTimestamp,
    SubmittedAtUtc,
    ReceivedAtUtc,
    Symbol,
    Side,
    SizeValue,
    Aum,
    ErrorCode,
    ErrorMessage,
    TradesJson
)
VALUES
(
    @ModelId,
    @OrderCode,
    @ScheduledTimestamp,
    @SubmittedAtUtc,
    @ReceivedAtUtc,
    @Symbol,
    @Side,
    @SizeValue,
    @Aum,
    @ErrorCode,
    @ErrorMessage,
    @TradesJson
);";

            foreach (var order in response.Orders)
            {
                var symbolKey = (order.Symbol ?? string.Empty).Trim().ToUpperInvariant();
                requestLookup.TryGetValue(symbolKey, out var requestItem);

                var symbol = !string.IsNullOrWhiteSpace(order.Symbol)
                    ? order.Symbol.Trim().ToUpperInvariant()
                    : requestItem?.symbol?.Trim().ToUpperInvariant();

                if (string.IsNullOrWhiteSpace(symbol))
                {
                    continue;
                }

                var sideValue = !string.IsNullOrWhiteSpace(order.Side)
                    ? order.Side.Trim().ToUpperInvariant()
                    : requestItem?.side?.Trim().ToUpperInvariant() ?? string.Empty;

                var sizeValue = order.Size.HasValue
                    ? (decimal?)order.Size.Value
                    : requestItem?.size is not null ? (decimal?)requestItem.size.value : null;

                var tradesJson = order.Trades is { Count: > 0 }
                    ? JsonSerializer.Serialize(order.Trades, OrderTradeJsonOptions)
                    : null;

                var resolvedOrderCode = ResolveOrderCode(order.Code, order.Symbol, orderCodeLookup)
                    ?? requestItem?.code?.Trim()
                    ?? string.Empty;

                var parameters = new
                {
                    ModelId = TargetModelId,
                    OrderCode = resolvedOrderCode,
                    ScheduledTimestamp = scheduledTimestamp,
                    SubmittedAtUtc = submittedUtc,
                    ReceivedAtUtc = receivedUtc,
                    Symbol = symbol,
                    Side = sideValue,
                    SizeValue = sizeValue,
                    Aum = aumValue,
                    ErrorCode = order.Error?.Code,
                    ErrorMessage = order.Error?.Message,
                    TradesJson = tradesJson
                };

                var definition = new CommandDefinition(insertSql, parameters, cancellationToken: cancellationToken);
                await connection.ExecuteAsync(definition);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to persist Wakett order response results.");
        }
    }

    protected virtual async Task<IReadOnlyCollection<string>> LoadExistingOrderSymbolsAsync(
        IDbConnection connection,
        DateTimeOffset scheduledTimestamp,
        IEnumerable<(int SecurityId, WakettOrderItem Order)> builtOrders,
        CancellationToken cancellationToken)
    {
        var symbols = builtOrders
            .Select(order => order.Order.symbol)
            .Where(symbol => !string.IsNullOrWhiteSpace(symbol))
            .Select(symbol => symbol!.Trim().ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (symbols.Length == 0)
        {
            return Array.Empty<string>();
        }

        var selectSql = $@"SELECT Symbol FROM {_orderTable}
WHERE ModelId = @ModelId AND ScheduledTimestamp = @ScheduledTimestamp AND Symbol IN @Symbols;";

        try
        {
            var definition = new CommandDefinition(
                selectSql,
                new
                {
                    ModelId = TargetModelId,
                    ScheduledTimestamp = scheduledTimestamp,
                    Symbols = symbols
                },
                cancellationToken: cancellationToken);

            var existing = await connection.QueryAsync<string>(definition);

            return existing
                .Where(symbol => !string.IsNullOrWhiteSpace(symbol))
                .Select(symbol => symbol.Trim().ToUpperInvariant())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load existing Wakett order symbols.");
            return Array.Empty<string>();
        }
    }

    private static DateTimeOffset ResolveScheduledTimestamp(DateTime orderTimestampUtc)
    {
        var formattedTimestamp = FormatTimestamp(orderTimestampUtc);
        return TryParseResponseTimestamp(formattedTimestamp, out var parsedTimestamp)
            ? parsedTimestamp
            : new DateTimeOffset(DateTime.SpecifyKind(orderTimestampUtc, DateTimeKind.Utc));
    }

    protected virtual async Task LogTradingLimitBreachesAsync(
        IDbConnection connection,
        IReadOnlyList<TradingLimitBreachResult> breaches,
        IReadOnlyList<(int SecurityId, WakettOrderItem Order)> orders,
        double? aum,
        CancellationToken cancellationToken)
    {
        if (breaches.Count == 0)
        {
            return;
        }

        try
        {
            var orderSnapshots = orders
                .Select(order => new TradingLimitOrderSnapshot(
                    order.SecurityId,
                    order.Order.symbol ?? string.Empty,
                    order.Order.side ?? string.Empty,
                    order.Order.size?.type ?? string.Empty,
                    order.Order.size?.value ?? 0d))
                .ToList();

            var ordersJson = orderSnapshots.Count > 0
                ? JsonSerializer.Serialize(orderSnapshots, OrderTradeJsonOptions)
                : null;

            var aumValue = aum.HasValue ? (decimal?)aum.Value : null;

            var insertSql = $@"INSERT INTO {_tradingLimitBreachTable}
(
    ModelId,
    LimitType,
    LimitValue,
    ObservedValue,
    Details,
    OrdersJson,
    Aum
)
VALUES
(
    @ModelId,
    @LimitType,
    @LimitValue,
    @ObservedValue,
    @Details,
    @OrdersJson,
    @Aum
);";

            foreach (var breach in breaches)
            {
                var definition = new CommandDefinition(
                    insertSql,
                    new
                    {
                        ModelId = TargetModelId,
                        breach.LimitType,
                        breach.LimitValue,
                        breach.ObservedValue,
                        Details = breach.Message,
                        OrdersJson = ordersJson,
                        Aum = aumValue
                    },
                    cancellationToken: cancellationToken);

                await connection.ExecuteAsync(definition);
            }

            ShowBreachWarning(breaches);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to log trading limit breach report.");
        }
    }

    private void ShowBreachWarning(IReadOnlyList<TradingLimitBreachResult> breaches)
    {
        if (!OperatingSystem.IsWindows() || breaches.Count == 0)
        {
            return;
        }

        try
        {
            var builder = new StringBuilder();
            builder.AppendLine("Trading limit breach detected. Orders were not sent.");

            foreach (var breach in breaches)
            {
                builder.Append("• ");
                builder.Append(breach.LimitType);
                builder.Append(": observed ");
                builder.Append(breach.ObservedValue.ToString("0.######", CultureInfo.InvariantCulture));
                builder.Append(" vs limit ");
                builder.AppendLine(breach.LimitValue.ToString("0.######", CultureInfo.InvariantCulture));
                if (!string.IsNullOrWhiteSpace(breach.Message))
                {
                    builder.AppendLine(breach.Message);
                }
            }

            var message = builder.ToString();
            var type = Type.GetType("System.Windows.Forms.MessageBox, System.Windows.Forms");
            type?.GetMethod("Show", new[] { typeof(string), typeof(string) })?
                .Invoke(null, new object[] { message, "Trading Limit Breach" });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to display trading limit breach warning.");
        }
    }

    internal static string FormatTimestamp(DateTime orderTimestampUtc)
    {
        var utc = DateTime.SpecifyKind(orderTimestampUtc, DateTimeKind.Utc);
        var local = TimeZoneInfo.ConvertTimeFromUtc(utc, NewYorkZone);

        var offset = NewYorkZone.GetUtcOffset(local);
        var sign = offset < TimeSpan.Zero ? "-" : "+";
        var abs = offset.Duration();
        var offsetString = $"{sign}{abs.Hours:00}{abs.Minutes:00}";
        return $"{local:yyyy-MM-dd HH:mm:ss.fff}{offsetString}";
    }

    private static bool TryParseResponseTimestamp(string? value, out DateTimeOffset parsedTimestamp)
    {
        parsedTimestamp = default;

        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var normalized = NormalizeWakettTimestamp(value.Trim());
        var formats = new[]
        {
            "yyyy-MM-dd HH:mm:ss.fffzzz",
            "yyyy-MM-dd HH:mm:sszzz"
        };

        foreach (var format in formats)
        {
            if (DateTimeOffset.TryParseExact(
                    normalized,
                    format,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out parsedTimestamp))
            {
                return true;
            }
        }

        return false;
    }

    private static string NormalizeWakettTimestamp(string value)
    {
        if (value.Length >= 5)
        {
            var signIndex = value.Length - 5;
            if ((value[signIndex] == '+' || value[signIndex] == '-') && value[^3] != ':')
            {
                return value.Insert(value.Length - 2, ":");
            }
        }

        return value;
    }

    private static bool TryGetSessionBounds(string sessionKey, bool useUsHolidaySchedule, out (TimeSpan Start, TimeSpan End) bounds)
    {
        bounds = default;

        if (string.IsNullOrWhiteSpace(sessionKey))
        {
            return false;
        }

        var normalized = sessionKey.Trim().ToUpperInvariant();
        if (string.Equals(normalized, "US", StringComparison.Ordinal))
        {
            bounds = ResolveUsSessionBounds(useUsHolidaySchedule);
            return true;
        }

        return SessionBounds.TryGetValue(normalized, out bounds);
    }

    private static (TimeSpan Start, TimeSpan End) ResolveUsSessionBounds(bool useUsHolidaySchedule)
        => useUsHolidaySchedule ? UsBankHolidaySessionBounds : UsSessionBounds;

    private bool TryResolveSessionBounds(string? sessionKey, out (TimeSpan Start, TimeSpan End) bounds)
        => TryGetSessionBounds(sessionKey ?? string.Empty, _useUsHolidaySchedule, out bounds);

    internal static DateTime CalculateOrderTimestamp(DateTime utcNow, string sessionKey, bool useUsHolidaySchedule = false)
    {
        var bounds = TryGetSessionBounds(sessionKey, useUsHolidaySchedule, out var resolved)
            ? resolved
            : ResolveUsSessionBounds(useUsHolidaySchedule);

        var duration = bounds.End - bounds.Start;
        if (duration <= TimeSpan.Zero)
        {
            duration += TimeSpan.FromDays(1);
        }

        var zone = NewYorkZone;
        var localNow = TimeZoneInfo.ConvertTimeFromUtc(utcNow, zone);
        var sessionStart = SkipWeekend(localNow.Date + bounds.Start);
        var sessionEnd = sessionStart + duration;

        if (localNow < sessionStart)
        {
            localNow = sessionStart;
        }
        else if (localNow > sessionEnd)
        {
            sessionStart = SkipWeekend(sessionStart.AddDays(1));
            sessionEnd = sessionStart + duration;
            localNow = sessionStart;
        }

        while (true)
        {
            var nextSlot = GetNextQuarterHourTimestamp(localNow);
            if (nextSlot <= sessionEnd)
            {
                return TimeZoneInfo.ConvertTimeToUtc(nextSlot, zone);
            }

            sessionStart = SkipWeekend(sessionStart.AddDays(1));
            sessionEnd = sessionStart + duration;
            localNow = sessionStart;
        }
    }

    private static DateTime SkipWeekend(DateTime candidate)
    {
        while (candidate.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday)
        {
            candidate = candidate.AddDays(1);
        }

        return candidate;
    }

    private static DateTime GetNextQuarterHourTimestamp(DateTime local)
    {
        var scheduleMinutes = new[] { 6, 21, 36, 51 };
        var candidateHour = local.Hour;

        foreach (var minute in scheduleMinutes)
        {
            var candidate = new DateTime(local.Year, local.Month, local.Day, candidateHour, minute, 0, local.Kind);
            if (candidate > local)
            {
                return candidate;
            }
        }

        return new DateTime(local.Year, local.Month, local.Day, candidateHour, scheduleMinutes[0], 0, local.Kind)
            .AddHours(1);
    }

    private TimeSpan ResolveBarInterval(
        IReadOnlyList<NettedWeightRow> weights,
        DateTime latestBarTimeUtc,
        int? configuredMinutes = null)
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

        if (configuredMinutes is > 0)
        {
            return TimeSpan.FromMinutes(configuredMinutes.Value);
        }

        var configured = Environment.GetEnvironmentVariable("WAKETT_BAR_INTERVAL_MINUTES")
            ?? _configuration["ExternalApis:WakettApi:BarIntervalMinutes"];

        if (int.TryParse(configured, NumberStyles.Integer, CultureInfo.InvariantCulture, out var minutes) && minutes > 0)
        {
            return TimeSpan.FromMinutes(minutes);
        }

        var fallbackMinutes = Math.Max(1, _priceBarOptions.TimeframeMinute);
        return TimeSpan.FromMinutes(fallbackMinutes);
    }

    protected virtual string ResolveTradingSession(DateTime barTimeUtc)
    {
        var configured = Environment.GetEnvironmentVariable("WAKETT_SESSION")
            ?? _configuration["ExternalApis:WakettApi:Session"];

        if (!string.IsNullOrWhiteSpace(configured))
        {
            var normalized = configured.Trim().ToUpperInvariant();
            if (AllowedTradingSessions.Contains(normalized))
            {
                return normalized;
            }

            if (SessionBounds.ContainsKey(normalized))
            {
                _logger.LogWarning(
                    "Configured trading session {ConfiguredSession} is not enabled. Falling back to allowed sessions EU/US/EUUS.",
                    configured);
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
                if (AllowedTradingSessions.Contains(normalized))
                {
                    return normalized;
                }
            }
        }

        var local = TimeZoneInfo.ConvertTimeFromUtc(barTimeUtc, NewYorkZone);
        foreach (var session in TradingSessionPreference)
        {
            if (TryResolveSessionBounds(session, out var bounds) && IsWithinSession(local, bounds))
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

    private TimeSpan CurrentUsSessionEndOrderTime => _useUsHolidaySchedule
        ? UsBankHolidaySessionEndOrderTime
        : UsSessionEndOrderTime;

    private bool IsPastUsHolidayCutoff(DateTime utcNow)
    {
        var local = TimeZoneInfo.ConvertTimeFromUtc(utcNow, NewYorkZone);
        return local.TimeOfDay >= UsBankHolidayCutoffTime;
    }

    private bool IsEndOfDayOrder(DateTime orderTimestampUtc)
    {
        var local = TimeZoneInfo.ConvertTimeFromUtc(orderTimestampUtc, NewYorkZone);
        return local.TimeOfDay == CurrentUsSessionEndOrderTime;
    }

    protected virtual async Task<IReadOnlyList<NettedWeightRow>> LoadLatestNettedWeightsAsync(
        IDbConnection connection,
        IReadOnlyCollection<int> modelIds,
        CancellationToken cancellationToken)
    {
        if (modelIds.Count == 0)
        {
            return Array.Empty<NettedWeightRow>();
        }

        var sql = $@"SELECT TOP (5000)
    nw.SecurityId,
    nw.ModelId,
    nw.BarTimeUtc,
    nw.ModelRunId,
    nw.Weight
FROM {_nettedWeightTable} nw
WHERE nw.ModelId IN @ModelIds
ORDER BY nw.BarTimeUtc DESC, nw.SecurityId";

        var definition = new CommandDefinition(
            sql,
            new { ModelIds = modelIds },
            cancellationToken: cancellationToken);

        var rows = await connection.QueryAsync<NettedWeightRow>(definition);
        return rows.ToList();
    }

    protected virtual async Task<ModelScheduleRow?> LoadModelScheduleAsync(
        IDbConnection connection,
        int modelId,
        CancellationToken cancellationToken)
    {
        var sql = $@"SELECT TOP (1)
    BarSize,
    Offset
FROM {_modelTable}
WHERE ModelId = @ModelId
ORDER BY ModelId";

        var definition = new CommandDefinition(
            sql,
            new { ModelId = modelId },
            cancellationToken: cancellationToken);

        var row = await connection.QueryFirstOrDefaultAsync<ModelScheduleRow>(definition);
        return row;
    }

    protected virtual async Task<TradingLimitRow?> LoadTradingLimitsAsync(
        IDbConnection connection,
        CancellationToken cancellationToken)
    {
        var sql = $@"SELECT TOP (1)
    TradingLimitId,
    ModelId,
    SingleTradeGrossLimit,
    PortfolioGrossLimit,
    PortfolioNetLimit,
    SingleTradeTurnoverLimit,
    TotalTurnoverLimit
FROM {_tradingLimitTable}
WHERE ModelId = @ModelId
ORDER BY TradingLimitId DESC;";

        var definition = new CommandDefinition(
            sql,
            new { ModelId = TargetModelId },
            cancellationToken: cancellationToken);

        var row = await connection.QueryFirstOrDefaultAsync<TradingLimitRow>(definition);
        return row;
    }

    private bool IsBarRecentEnough(
        DateTime barTimeUtc,
        DateTime utcNow,
        TimeSpan barInterval,
        string? sessionKey = null,
        int? modelId = null)
    {
        var zone = CentralEuropeZone;
        var barLocal = TimeZoneInfo.ConvertTimeFromUtc(barTimeUtc, zone);
        var nowLocal = TimeZoneInfo.ConvertTimeFromUtc(utcNow, zone);

        if (barLocal.Date < nowLocal.Date)
        {
            _logger.LogInformation(
                "Using previous day's netted weights from {BarTimeUtc:O} for first trade of the day.",
                barTimeUtc);
            return true;
        }

        var allowedStaleness = barInterval > TimeSpan.Zero
            ? barInterval
            : TimeSpan.FromMinutes(Math.Max(1, _priceBarOptions.TimeframeMinute));

        if (utcNow - barTimeUtc > allowedStaleness)
        {
            var sessionKeyNormalized = sessionKey?.Trim().ToUpperInvariant();
            if (!string.IsNullOrWhiteSpace(sessionKeyNormalized) && TryResolveSessionBounds(sessionKeyNormalized, out var bounds))
            {
                var barLocalNy = TimeZoneInfo.ConvertTimeFromUtc(barTimeUtc, NewYorkZone);
                var nowLocalNy = TimeZoneInfo.ConvertTimeFromUtc(utcNow, NewYorkZone);
                var (_, sessionEnd) = GetSessionWindow(barLocalNy, bounds);

                if (nowLocalNy > sessionEnd)
                {
                    _logger.LogInformation(
                        "Using latest available netted weights from {BarTimeUtc:O} for model {ModelId} after session end {SessionEndLocal:O}.",
                        barTimeUtc,
                        modelId ?? TargetModelId,
                        sessionEnd);
                    return true;
                }
            }

            _logger.LogWarning(
                "Latest netted weights are stale for model {ModelId}. Last bar: {BarTimeUtc:O}, now: {NowUtc:O}.",
                modelId ?? TargetModelId,
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
        var sql = $@"SELECT SecurityId, Symbol
FROM {_securityTable}
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

    private IReadOnlyCollection<CurrencyPair> LoadConfiguredBasePairs()
    {
        var configured = _configuration
            .GetSection("ExternalApis:WakettApi:BasePairs")
            .Get<string[]>() ?? Array.Empty<string>();

        var pairs = new List<CurrencyPair>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var entry in configured)
        {
            if (!CurrencyPairParser.TryParse(entry, out var pair))
            {
                _logger.LogWarning(
                    "Skipping invalid Wakett base pair configuration value '{Value}'.",
                    entry);
                continue;
            }

            var key = $"{pair.BaseCurrency}{pair.QuoteCurrency}";
            if (!seen.Add(key))
            {
                continue;
            }

            pairs.Add(pair);
        }

        return pairs;
    }

    private IReadOnlyDictionary<string, decimal> LoadPairUsdLimits()
    {
        var pairs = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);

        var configured = _configuration
            .GetSection("ExternalApis:WakettApi:PairUsdLimits")
            .GetChildren();

        foreach (var entry in configured)
        {
            if (string.IsNullOrWhiteSpace(entry.Key))
            {
                continue;
            }

            var key = entry.Key.Replace("/", string.Empty).Trim().ToUpperInvariant();
            if (key.Length != 6)
            {
                _logger.LogWarning(
                    "Skipping pair USD limit entry '{Key}' because normalized key '{Normalized}' is invalid.",
                    entry.Key,
                    key);
                continue;
            }

            if (!decimal.TryParse(entry.Value, NumberStyles.Any, CultureInfo.InvariantCulture, out var limit)
                || limit <= 0m)
            {
                _logger.LogWarning(
                    "Skipping pair USD limit entry '{Key}' with invalid value '{Value}'.",
                    entry.Key,
                    entry.Value);
                continue;
            }

            pairs[key] = limit;
        }

        return pairs;
    }

    private List<(int SecurityId, WakettOrderItem Order)> BuildOrders(
        IEnumerable<NettedWeightRow> weights,
        IReadOnlyDictionary<int, string> symbolMap,
        DateTime orderTimestampUtc,
        IReadOnlyCollection<CurrencyPair> configuredBasePairs)
    {
        var adjustedWeights = ApplyWeightOverrides(weights, symbolMap, orderTimestampUtc);

        var parsedSymbols = symbolMap
            .Select(pair => SymbolInfo.TryCreate(pair.Key, pair.Value, out var info) ? info : null)
            .Where(info => info is not null)
            .Cast<SymbolInfo>()
            .ToDictionary(info => info.SecurityId);

        if (parsedSymbols.Count == 0)
        {
            return new List<(int SecurityId, WakettOrderItem Order)>();
        }

        if (configuredBasePairs.Count == 0)
        {
            return new List<(int SecurityId, WakettOrderItem Order)>();
        }

        var configuredPairLookup = configuredBasePairs
            .GroupBy(pair => $"{pair.BaseCurrency}{pair.QuoteCurrency}", StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.First(), StringComparer.OrdinalIgnoreCase);

        var symbolPairLookup = parsedSymbols.Values
            .GroupBy(info => $"{info.BaseCurrency}{info.QuoteCurrency}", StringComparer.OrdinalIgnoreCase)
            .ToDictionary(
                group => group.Key,
                group => group.OrderBy(info => info.SecurityId).First(),
                StringComparer.OrdinalIgnoreCase);

        var exposures = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);

        foreach (var weight in adjustedWeights)
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
            var flatSymbols = configuredPairLookup.Keys
                .Select(key => symbolPairLookup.TryGetValue(key, out var info) ? info : null)
                .Where(info => info is not null)
                .Cast<SymbolInfo>()
                .OrderBy(info => info.SecurityId)
                .ToList();

            if (flatSymbols.Count == 0)
            {
                flatSymbols = adjustedWeights
                    .Select(weight => weight.SecurityId)
                    .Distinct()
                    .Select(id => parsedSymbols.TryGetValue(id, out var info) ? info : null)
                    .Where(info => info is not null)
                    .Cast<SymbolInfo>()
                    .OrderBy(info => info.SecurityId)
                    .ToList();
            }

            return BuildFlatOrders(flatSymbols, orderTimestampUtc);
        }

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

            var baseKey = $"{currency}USD";
            var quoteKey = $"USD{currency}";

            if (configuredPairLookup.ContainsKey(baseKey) && symbolPairLookup.TryGetValue(baseKey, out var basePair))
            {
                orderDescriptors.Add((basePair.SecurityId, basePair, exposure));
                continue;
            }

            if (configuredPairLookup.ContainsKey(quoteKey) && symbolPairLookup.TryGetValue(quoteKey, out var quotePair))
            {
                orderDescriptors.Add((quotePair.SecurityId, quotePair, -exposure));
            }
        }

        var result = new List<(int SecurityId, WakettOrderItem Order)>();

        foreach (var order in orderDescriptors.Where(order => order.Weight != 0m).OrderBy(order => order.SecurityId))
        {
            var formatted = order.Info.FormattedSymbol;
            var side = order.Weight > 0 ? "BUY" : "SELL";
            var value = Math.Abs((double)order.Weight);

            var orderCode = BuildOrderCode(order.SecurityId, orderTimestampUtc);

            result.Add((order.SecurityId, new WakettOrderItem
            {
                symbol = formatted,
                side = side,
                code = orderCode,
                size = new WakettOrderSize
                {
                    type = "percentage",
                    value = value
                }
            }));
        }

        return result;
    }

    private List<(int SecurityId, WakettOrderItem Order)> BuildEndOfDayOrders(
        IReadOnlyDictionary<int, string> symbolMap,
        IReadOnlyCollection<CurrencyPair> configuredBasePairs,
        DateTime orderTimestampUtc)
    {
        var parsedSymbols = symbolMap
            .Select(pair => SymbolInfo.TryCreate(pair.Key, pair.Value, out var info) ? info : null)
            .Where(info => info is not null)
            .Cast<SymbolInfo>()
            .ToDictionary(info => info.SecurityId);

        if (parsedSymbols.Count == 0 || configuredBasePairs.Count == 0)
        {
            return new List<(int SecurityId, WakettOrderItem Order)>();
        }

        var symbolPairLookup = parsedSymbols.Values
            .GroupBy(info => $"{info.BaseCurrency}{info.QuoteCurrency}", StringComparer.OrdinalIgnoreCase)
            .ToDictionary(
                group => group.Key,
                group => group.OrderBy(info => info.SecurityId).First(),
                StringComparer.OrdinalIgnoreCase);

        var endOfDaySymbols = configuredBasePairs
            .Select(pair => $"{pair.BaseCurrency}{pair.QuoteCurrency}")
            .Select(key => symbolPairLookup.TryGetValue(key, out var info) ? info : null)
            .Where(info => info is not null)
            .Cast<SymbolInfo>()
            .OrderBy(info => info.SecurityId)
            .ToList();

        return BuildFlatOrders(endOfDaySymbols, orderTimestampUtc);
    }

    private List<(int SecurityId, WakettOrderItem Order)> ApplyPairUsdLimits(
        List<(int SecurityId, WakettOrderItem Order)> builtOrders,
        IReadOnlyDictionary<string, decimal> pairUsdLimits,
        double aum)
    {
        if (builtOrders.Count == 0 || pairUsdLimits.Count == 0 || aum <= 0d)
        {
            return builtOrders;
        }

        var aumDecimal = (decimal)aum;
        var result = new List<(int SecurityId, WakettOrderItem Order)>(builtOrders.Count);

        foreach (var entry in builtOrders)
        {
            var order = entry.Order;
            var size = order.size;
            var symbolKey = order.symbol?.Replace("/", string.Empty).Trim().ToUpperInvariant();

            if (size is null || string.IsNullOrWhiteSpace(symbolKey) || !pairUsdLimits.TryGetValue(symbolKey, out var usdLimit))
            {
                result.Add(entry);
                continue;
            }

            var limitWeight = usdLimit / aumDecimal;
            var currentWeight = (decimal)size.value;

            if (currentWeight <= limitWeight)
            {
                result.Add(entry);
                continue;
            }

            var adjustedValue = (double)limitWeight;

            _logger.LogInformation(
                "Capping {Symbol} weight from {CurrentWeight} to {AdjustedWeight} using USD limit {UsdLimit} and AUM {Aum}.",
                order.symbol,
                currentWeight,
                limitWeight,
                usdLimit,
                aum);

            var adjustedOrder = new WakettOrderItem
            {
                symbol = order.symbol,
                side = order.side,
                code = order.code,
                size = new WakettOrderSize
                {
                    type = size.type,
                    value = adjustedValue
                }
            };

            result.Add((entry.SecurityId, adjustedOrder));
        }

        return result;
    }

    protected virtual IEnumerable<NettedWeightRow> ApplyWeightOverrides(
        IEnumerable<NettedWeightRow> weights,
        IReadOnlyDictionary<int, string> symbolMap,
        DateTime orderTimestampUtc)
    {
        var weightList = weights.ToList();

        if (weightList.Count == 0)
        {
            return weightList;
        }

        var localTime = TimeZoneInfo.ConvertTimeFromUtc(orderTimestampUtc, NewYorkZone);

        if (localTime.TimeOfDay < NyWeightOverrideTime)
        {
            return weightList;
        }

        var nzdusdIds = symbolMap
            .Where(pair =>
                !string.IsNullOrWhiteSpace(pair.Value) &&
                string.Equals(
                    pair.Value.Trim().Replace("/", string.Empty),
                    "NZDUSD",
                    StringComparison.OrdinalIgnoreCase))
            .Select(pair => pair.Key)
            .ToHashSet();

        if (nzdusdIds.Count == 0)
        {
            return weightList;
        }

        _logger.LogInformation(
            "Zeroing NZDUSD weights for order timestamp {OrderTimestampUtc:O} after {CutoffLocal} NY.",
            orderTimestampUtc,
            NyWeightOverrideTime);

        return weightList
            .Select(weight => nzdusdIds.Contains(weight.SecurityId) ? weight with { Weight = 0m } : weight)
            .ToList();
    }

    private static List<(int SecurityId, WakettOrderItem Order)> BuildFlatOrders(
        IEnumerable<SymbolInfo> orderedSymbols,
        DateTime orderTimestampUtc)
    {
        var result = new List<(int SecurityId, WakettOrderItem Order)>();

        foreach (var symbol in orderedSymbols)
        {
            var formatted = symbol.FormattedSymbol;

            result.Add((symbol.SecurityId, new WakettOrderItem
            {
                symbol = formatted,
                side = "BUY",
                code = BuildOrderCode(symbol.SecurityId, orderTimestampUtc),
                size = new WakettOrderSize
                {
                    type = "percentage",
                    value = 0d
                }
            }));
        }

        return result;
    }

    private static IReadOnlyList<TradingLimitBreachResult> EvaluateTradingLimitBreaches(
        TradingLimitRow limits,
        IReadOnlyList<(int SecurityId, WakettOrderItem Order)> builtOrders,
        double? aum)
    {
        if (builtOrders.Count == 0)
        {
            return Array.Empty<TradingLimitBreachResult>();
        }

        var metrics = new List<TradingOrderMetric>();

        foreach (var entry in builtOrders)
        {
            if (entry.Order.size is null)
            {
                continue;
            }

            var symbol = entry.Order.symbol?.Trim() ?? string.Empty;
            var side = entry.Order.side?.Trim() ?? string.Empty;
            var signedSize = (decimal)entry.Order.size.value;

            if (string.Equals(side, "SELL", StringComparison.OrdinalIgnoreCase))
            {
                signedSize = -signedSize;
            }

            metrics.Add(new TradingOrderMetric(entry.SecurityId, symbol, side, signedSize));
        }

        if (metrics.Count == 0)
        {
            return Array.Empty<TradingLimitBreachResult>();
        }

        var breaches = new List<TradingLimitBreachResult>();
        var aumDecimal = aum.HasValue ? (decimal?)aum.Value : null;

        string FormatTurnoverMessage(
            string prefix,
            decimal observedWeight,
            decimal limitWeight,
            decimal? aumValue)
        {
            if (aumValue.HasValue)
            {
                var notional = observedWeight * aumValue.Value;
                return string.Format(
                    CultureInfo.InvariantCulture,
                    "{0} ({1} > {2}). Equivalent notional {3} using AUM {4}.",
                    prefix,
                    observedWeight,
                    limitWeight,
                    notional,
                    aumValue.Value);
            }

            return string.Format(
                CultureInfo.InvariantCulture,
                "{0} ({1} > {2}).",
                prefix,
                observedWeight,
                limitWeight);
        }

        TradingOrderMetric FindLargest(Func<TradingOrderMetric, decimal> selector)
        {
            var candidate = metrics[0];
            var candidateValue = selector(candidate);

            for (var i = 1; i < metrics.Count; i++)
            {
                var metric = metrics[i];
                var value = selector(metric);
                if (value > candidateValue)
                {
                    candidate = metric;
                    candidateValue = value;
                }
            }

            return candidate;
        }

        if (limits.SingleTradeGrossLimit is decimal singleGrossLimit)
        {
            var worst = FindLargest(metric => metric.AbsoluteSize);
            var observed = worst.AbsoluteSize;
            if (observed > singleGrossLimit)
            {
                var message = string.Format(
                    CultureInfo.InvariantCulture,
                    "Order for {0} exceeds single trade gross limit ({1} > {2}).",
                    worst.Symbol,
                    observed,
                    singleGrossLimit);
                breaches.Add(new TradingLimitBreachResult(
                    "SingleTradeGrossLimit",
                    singleGrossLimit,
                    observed,
                    message));
            }
        }

        if (limits.PortfolioGrossLimit is decimal portfolioGrossLimit)
        {
            var observed = metrics.Sum(metric => metric.AbsoluteSize);
            if (observed > portfolioGrossLimit)
            {
                var message = string.Format(
                    CultureInfo.InvariantCulture,
                    "Total gross exposure {0} exceeds portfolio gross limit {1}.",
                    observed,
                    portfolioGrossLimit);
                breaches.Add(new TradingLimitBreachResult(
                    "PortfolioGrossLimit",
                    portfolioGrossLimit,
                    observed,
                    message));
            }
        }

        if (limits.PortfolioNetLimit is decimal portfolioNetLimit)
        {
            var observed = Math.Abs(metrics.Sum(metric => metric.SignedSize));
            if (observed > portfolioNetLimit)
            {
                var message = string.Format(
                    CultureInfo.InvariantCulture,
                    "Net exposure {0} exceeds portfolio net limit {1}.",
                    observed,
                    portfolioNetLimit);
                breaches.Add(new TradingLimitBreachResult(
                    "PortfolioNetLimit",
                    portfolioNetLimit,
                    observed,
                    message));
            }
        }

        if (limits.SingleTradeTurnoverLimit is decimal singleTradeTurnoverLimit)
        {
            var worstTurnover = FindLargest(metric => metric.AbsoluteSize);
            var observedWeight = worstTurnover.AbsoluteSize;
            if (observedWeight > singleTradeTurnoverLimit)
            {
                var message = FormatTurnoverMessage(
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "Order for {0} exceeds single trade turnover weight limit",
                        worstTurnover.Symbol),
                    observedWeight,
                    singleTradeTurnoverLimit,
                    aumDecimal);

                breaches.Add(new TradingLimitBreachResult(
                    "SingleTradeTurnoverLimit",
                    singleTradeTurnoverLimit,
                    observedWeight,
                    message));
            }
        }

        if (limits.TotalTurnoverLimit is decimal totalTurnoverLimit)
        {
            var observedWeight = metrics.Sum(metric => metric.AbsoluteSize);
            if (observedWeight > totalTurnoverLimit)
            {
                var message = FormatTurnoverMessage(
                    "Total turnover weight exceeds limit",
                    observedWeight,
                    totalTurnoverLimit,
                    aumDecimal);

                breaches.Add(new TradingLimitBreachResult(
                    "TotalTurnoverLimit",
                    totalTurnoverLimit,
                    observedWeight,
                    message));
            }
        }

        return breaches;
    }

    private static string? ResolveOrderCode(
        string? responseCode,
        string? responseSymbol,
        IReadOnlyDictionary<string, string?> orderCodeLookup)
    {
        if (!string.IsNullOrWhiteSpace(responseCode))
        {
            return responseCode.Trim();
        }

        if (!string.IsNullOrWhiteSpace(responseSymbol))
        {
            var symbolKey = responseSymbol.Trim().ToUpperInvariant();
            if (orderCodeLookup.TryGetValue(symbolKey, out var code) && !string.IsNullOrWhiteSpace(code))
            {
                return code.Trim();
            }
        }

        return null;
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

    private sealed record ModelDefinition(int ModelId, int? TimeframeMinutes, string? Session);

    private sealed record ModelSnapshot(
        int ModelId,
        string? SessionKey,
        int? TimeframeMinutes,
        TimeSpan BarInterval,
        DateTime BarTimeUtc,
        IReadOnlyList<NettedWeightRow> LatestWeights);

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

    private static TimeZoneInfo NewZealandZone => TimeZoneInfo.FindSystemTimeZoneById(
        RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "New Zealand Standard Time" : "Pacific/Auckland");

    private sealed record TradingLimitOrderSnapshot(int SecurityId, string Symbol, string Side, string SizeType, double SizeValue);

    private readonly record struct TradingOrderMetric(int SecurityId, string Symbol, string Side, decimal SignedSize)
    {
        public decimal AbsoluteSize => Math.Abs(SignedSize);
    }

    protected sealed record TradingLimitBreachResult(string LimitType, decimal LimitValue, decimal ObservedValue, string Message);

    protected sealed record TradingLimitRow
    {
        public int TradingLimitId { get; init; }

        public int ModelId { get; init; }

        public decimal? SingleTradeGrossLimit { get; init; }

        public decimal? PortfolioGrossLimit { get; init; }

        public decimal? PortfolioNetLimit { get; init; }

        public decimal? SingleTradeTurnoverLimit { get; init; }

        public decimal? TotalTurnoverLimit { get; init; }
    }

    protected sealed record NettedWeightRow
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
