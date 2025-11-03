using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using Dapper;
using Microsoft.Extensions.Logging;
using TradingDaemon.Data;
using TradingDaemon.Models;

namespace TradingDaemon.Services;

public class WakettTradeFetcher
{
    private static readonly string[] TimestampFormats =
    {
        "yyyy-MM-dd HH:mm:ss.fff zzz",
        "yyyy-MM-dd HH:mm:ss.fffzzz",
        "yyyy-MM-dd HH:mm:ss zzz",
        "yyyy-MM-dd HH:mm:sszzz",
        "yyyy-MM-dd'T'HH:mm:ss.fff zzz",
        "yyyy-MM-dd'T'HH:mm:ss.fffzzz",
        "yyyy-MM-dd'T'HH:mm:ss zzz",
        "yyyy-MM-dd'T'HH:mm:sszzz"
    };

    private const string MergeSql = @"
MERGE [wakett].[Fill] AS target
USING (VALUES (
    @ExecuteId,
    @Account,
    @RequestedFrom,
    @RequestedTo,
    @RequestedStrategy,
    @PortfolioId,
    @Portfolio,
    @Alias,
    @Broker,
    @StrategyId,
    @SymbolId,
    @Symbol,
    @InstrumentId,
    @Reference,
    @CoreOrderId,
    @SubOrderId,
    @Label,
    @Side,
    @OrderPrice,
    @OrderId,
    @OrderTimestamp,
    @OrderSize,
    @OrderChannel,
    @ProviderId,
    @ProviderTimestamp,
    @ExecuteTimestamp,
    @EntitySize,
    @ExecuteSize,
    @ExecutePrice,
    @TradeTimestamp,
    @Event,
    @UserName,
    @Code,
    @RecordType,
    @Quote,
    @Amount,
    @Rate,
    @RecordedAtUtc
)) AS source (
    ExecuteId,
    Account,
    RequestedFrom,
    RequestedTo,
    RequestedStrategy,
    PortfolioId,
    Portfolio,
    Alias,
    Broker,
    StrategyId,
    SymbolId,
    Symbol,
    InstrumentId,
    Reference,
    CoreOrderId,
    SubOrderId,
    Label,
    Side,
    OrderPrice,
    OrderId,
    OrderTimestamp,
    OrderSize,
    OrderChannel,
    ProviderId,
    ProviderTimestamp,
    ExecuteTimestamp,
    EntitySize,
    ExecuteSize,
    ExecutePrice,
    TradeTimestamp,
    Event,
    UserName,
    Code,
    RecordType,
    Quote,
    Amount,
    Rate,
    RecordedAtUtc
)
ON target.ExecuteId = source.ExecuteId
    AND target.Account = source.Account
    AND ISNULL(target.SubOrderId, -2147483648) = ISNULL(source.SubOrderId, -2147483648)
    AND ISNULL(target.ExecuteTimestamp, '0001-01-01T00:00:00+00:00') = ISNULL(source.ExecuteTimestamp, '0001-01-01T00:00:00+00:00')
WHEN MATCHED THEN
    UPDATE SET
        Account = source.Account,
        RequestedFrom = source.RequestedFrom,
        RequestedTo = source.RequestedTo,
        RequestedStrategy = source.RequestedStrategy,
        PortfolioId = source.PortfolioId,
        Portfolio = source.Portfolio,
        Alias = source.Alias,
        Broker = source.Broker,
        StrategyId = source.StrategyId,
        SymbolId = source.SymbolId,
        Symbol = source.Symbol,
        InstrumentId = source.InstrumentId,
        Reference = source.Reference,
        CoreOrderId = source.CoreOrderId,
        SubOrderId = source.SubOrderId,
        Label = source.Label,
        Side = source.Side,
        OrderPrice = source.OrderPrice,
        OrderId = source.OrderId,
        OrderTimestamp = source.OrderTimestamp,
        OrderSize = source.OrderSize,
        OrderChannel = source.OrderChannel,
        ProviderId = source.ProviderId,
        ProviderTimestamp = source.ProviderTimestamp,
        ExecuteTimestamp = source.ExecuteTimestamp,
        EntitySize = source.EntitySize,
        ExecuteSize = source.ExecuteSize,
        ExecutePrice = source.ExecutePrice,
        TradeTimestamp = source.TradeTimestamp,
        Event = source.Event,
        UserName = source.UserName,
        Code = source.Code,
        RecordType = source.RecordType,
        Quote = source.Quote,
        Amount = source.Amount,
        Rate = source.Rate,
        UpdatedAtUtc = source.RecordedAtUtc
WHEN NOT MATCHED THEN
    INSERT (
        ExecuteId,
        Account,
        RequestedFrom,
        RequestedTo,
        RequestedStrategy,
        PortfolioId,
        Portfolio,
        Alias,
        Broker,
        StrategyId,
        SymbolId,
        Symbol,
        InstrumentId,
        Reference,
        CoreOrderId,
        SubOrderId,
        Label,
        Side,
        OrderPrice,
        OrderId,
        OrderTimestamp,
        OrderSize,
        OrderChannel,
        ProviderId,
        ProviderTimestamp,
        ExecuteTimestamp,
        EntitySize,
        ExecuteSize,
        ExecutePrice,
        TradeTimestamp,
        Event,
        UserName,
        Code,
        RecordType,
        Quote,
        Amount,
        Rate,
        CreatedAtUtc,
        UpdatedAtUtc
    )
    VALUES (
        source.ExecuteId,
        source.Account,
        source.RequestedFrom,
        source.RequestedTo,
        source.RequestedStrategy,
        source.PortfolioId,
        source.Portfolio,
        source.Alias,
        source.Broker,
        source.StrategyId,
        source.SymbolId,
        source.Symbol,
        source.InstrumentId,
        source.Reference,
        source.CoreOrderId,
        source.SubOrderId,
        source.Label,
        source.Side,
        source.OrderPrice,
        source.OrderId,
        source.OrderTimestamp,
        source.OrderSize,
        source.OrderChannel,
        source.ProviderId,
        source.ProviderTimestamp,
        source.ExecuteTimestamp,
        source.EntitySize,
        source.ExecuteSize,
        source.ExecutePrice,
        source.TradeTimestamp,
        source.Event,
        source.UserName,
        source.Code,
        source.RecordType,
        source.Quote,
        source.Amount,
        source.Rate,
        source.RecordedAtUtc,
        source.RecordedAtUtc
    )
OUTPUT $action;";

    private readonly WakettApiClient _client;
    private readonly DapperContext _context;
    private readonly ILogger<WakettTradeFetcher> _logger;
    private readonly TimeProvider _timeProvider;

    public WakettTradeFetcher(
        WakettApiClient client,
        DapperContext context,
        ILogger<WakettTradeFetcher> logger,
        TimeProvider? timeProvider = null)
    {
        _client = client;
        _context = context;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<WakettFillUploadResponse> FetchAndStoreAsync(
        FetchWakettFillsRequest request,
        CancellationToken cancellationToken = default)
    {
        if (request is null)
        {
            throw new ArgumentNullException(nameof(request));
        }

        var normalized = NormalizeRequest(request);

        _logger.LogInformation(
            "Requesting Wakett fills for account {Account} from {From} to {To} (strategy: {Strategy}).",
            normalized.Account,
            normalized.FromString,
            normalized.ToStringValue,
            normalized.Strategy ?? "<all>");

        var tradeRequest = new WakettTradeRequest
        {
            Account = normalized.Account,
            From = normalized.FromString,
            To = normalized.ToStringValue,
            Strategy = normalized.Strategy
        };

        var response = await _client.GetTradesAsync(tradeRequest, cancellationToken);
        if (response is null)
        {
            throw new WakettTradeFetcherException(
                "NoResponse",
                "The Wakett trading API returned an empty response when requesting fills.");
        }

        if (!string.Equals(response.Status, "OK", StringComparison.OrdinalIgnoreCase))
        {
            throw new WakettTradeFetcherException(response.Status, response.Message);
        }

        var executions = response.Data ?? new List<WakettTrade>();
        var recordedAtUtc = _timeProvider.GetUtcNow().UtcDateTime;
        var skipped = new List<WakettFillUploadSkippedRecord>();

        if (executions.Count == 0)
        {
            _logger.LogInformation(
                "The Wakett trading API returned no executions for account {Account} between {From} and {To}.",
                normalized.Account,
                normalized.FromString,
                normalized.ToStringValue);

            return new WakettFillUploadResponse(
                normalized.Account,
                normalized.FromString,
                normalized.ToStringValue,
                normalized.Strategy,
                response.Status,
                response.Message,
                0,
                0,
                0,
                recordedAtUtc,
                skipped);
        }

        using var connection = _context.CreateConnection();
        if (connection is DbConnection dbConnection)
        {
            await dbConnection.OpenAsync(cancellationToken);
        }
        else
        {
            connection.Open();
        }

        using var transaction = connection.BeginTransaction();
        var inserted = 0;
        var updated = 0;
        foreach (var trade in executions)
        {
            var item = CreateUploadItem(trade, normalized, recordedAtUtc, skipped);
            if (item is null)
            {
                continue;
            }

            var command = new CommandDefinition(
                MergeSql,
                item,
                transaction,
                cancellationToken: cancellationToken);

            var action = await connection.QuerySingleAsync<string>(command);
            if (string.Equals(action, "INSERT", StringComparison.OrdinalIgnoreCase))
            {
                inserted++;
            }
            else if (string.Equals(action, "UPDATE", StringComparison.OrdinalIgnoreCase))
            {
                updated++;
            }
        }

        transaction.Commit();

        _logger.LogInformation(
            "Persisted {Inserted} new and {Updated} existing Wakett fills for execute window {From}-{To}.",
            inserted,
            updated,
            normalized.FromString,
            normalized.ToStringValue);

        return new WakettFillUploadResponse(
            normalized.Account,
            normalized.FromString,
            normalized.ToStringValue,
            normalized.Strategy,
            response.Status,
            response.Message,
            executions.Count,
            inserted,
            updated,
            recordedAtUtc,
            skipped);
    }

    private WakettFillUploadItem? CreateUploadItem(
        WakettTrade trade,
        NormalizedRequest normalized,
        DateTime recordedAtUtc,
        List<WakettFillUploadSkippedRecord> skipped)
    {
        var executeId = trade.ExecuteId?.Trim();
        if (string.IsNullOrWhiteSpace(executeId))
        {
            skipped.Add(new WakettFillUploadSkippedRecord(
                "Execution is missing executeID.",
                null,
                trade.Symbol));
            return null;
        }

        var symbol = trade.Symbol?.Trim();
        if (string.IsNullOrWhiteSpace(symbol))
        {
            skipped.Add(new WakettFillUploadSkippedRecord(
                "Execution is missing symbol.",
                executeId,
                null));
            return null;
        }

        return new WakettFillUploadItem(
            executeId,
            normalized.Account,
            normalized.FromString,
            normalized.ToStringValue,
            normalized.Strategy,
            TrimOrNull(trade.PortfolioId),
            TrimOrNull(trade.Portfolio),
            TrimOrNull(trade.Alias),
            TrimOrNull(trade.Broker),
            TrimOrNull(trade.StrategyId),
            TrimOrNull(trade.SymbolId),
            symbol.ToUpperInvariant(),
            TrimOrNull(trade.InstrumentId),
            TrimOrNull(trade.Reference),
            trade.CoreOrderId,
            trade.SubOrderId,
            TrimOrNull(trade.Label),
            TrimOrNull(trade.Side)?.ToUpperInvariant(),
            ToDecimal(trade.Price),
            TrimOrNull(trade.OrderId),
            ParseTimestamp(trade.OrderTimestamp, "orderTS", executeId),
            ToDecimal(trade.OrderSize),
            TrimOrNull(trade.OrderChannel),
            TrimOrNull(trade.ProviderId),
            ParseTimestamp(trade.ProviderTimestamp, "providerTS", executeId),
            ParseTimestamp(trade.ExecuteTimestamp, "executeTS", executeId),
            ToDecimal(trade.EntitySize),
            ToDecimal(trade.ExecuteSize),
            ToDecimal(trade.ExecutePrice),
            ParseTimestamp(trade.TradeTimestamp, "tradets", executeId),
            TrimOrNull(trade.Event),
            TrimOrNull(trade.User),
            TrimOrNull(trade.Code),
            TrimOrNull(trade.Type),
            ToDecimal(trade.Quote),
            ToDecimal(trade.Amount),
            ToDecimal(trade.Rate),
            recordedAtUtc);
    }

    private static decimal? ToDecimal(double? value)
        => value.HasValue ? Convert.ToDecimal(value.Value, CultureInfo.InvariantCulture) : null;

    private DateTimeOffset? ParseTimestamp(string? raw, string fieldName, string executeId)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        var trimmed = raw.Trim();
        var normalized = NormalizeTimestamp(trimmed);

        if (DateTimeOffset.TryParseExact(
                normalized,
                TimestampFormats,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AllowWhiteSpaces,
                out var timestamp))
        {
            return timestamp;
        }

        _logger.LogWarning(
            "Unable to parse {Field} timestamp '{Value}' for Wakett executeID {ExecuteId}.",
            fieldName,
            raw,
            executeId);
        return null;
    }

    private static string NormalizeTimestamp(string value)
    {
        var builder = new StringBuilder(value);
        var plusIndex = value.LastIndexOf('+');
        var minusIndex = value.LastIndexOf('-');
        var index = Math.Max(plusIndex, minusIndex);
        if (index > 0 && index < value.Length - 1)
        {
            var offset = value[index..];
            if (offset.Length == 5 && (offset[0] == '+' || offset[0] == '-') && char.IsDigit(offset[1]) && char.IsDigit(offset[2]) && char.IsDigit(offset[3]) && char.IsDigit(offset[4]))
            {
                builder.Clear();
                builder.Append(value.AsSpan(0, index + 3));
                builder.Append(':');
                builder.Append(value.AsSpan(index + 3));
                return builder.ToString();
            }
        }

        return value;
    }

    private static string? TrimOrNull(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var trimmed = value.Trim();
        return trimmed.Length == 0 ? null : trimmed;
    }

    private static NormalizedRequest NormalizeRequest(FetchWakettFillsRequest request)
    {
        var account = TrimOrNull(request.Account)
            ?? throw new ArgumentException("Account must be provided.", nameof(request));

        var from = ParseDate(request.From, nameof(request.From));
        var to = ParseDate(request.To, nameof(request.To));
        if (to < from)
        {
            throw new ArgumentException("The 'to' date must be on or after the 'from' date.", nameof(request));
        }

        var strategy = TrimOrNull(request.Strategy);

        return new NormalizedRequest(
            account,
            from,
            to,
            from.ToString("yyyyMMdd", CultureInfo.InvariantCulture),
            to.ToString("yyyyMMdd", CultureInfo.InvariantCulture),
            strategy);
    }

    private static DateOnly ParseDate(string? value, string propertyName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException($"{propertyName} must be provided in YYYYMMDD format.", propertyName);
        }

        var trimmed = value.Trim();
        if (!DateOnly.TryParseExact(trimmed, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var date))
        {
            throw new ArgumentException($"{propertyName} must be provided in YYYYMMDD format.", propertyName);
        }

        return date;
    }

    private readonly record struct NormalizedRequest(
        string Account,
        DateOnly From,
        DateOnly To,
        string FromString,
        string ToStringValue,
        string? Strategy);
}

public sealed class WakettTradeFetcherException : Exception
{
    public WakettTradeFetcherException(string status, string message)
        : base(string.IsNullOrWhiteSpace(message) ? status : message)
    {
        Status = status;
    }

    public string Status { get; }
}

internal sealed record WakettFillUploadItem(
    string ExecuteId,
    string Account,
    string RequestedFrom,
    string RequestedTo,
    string? RequestedStrategy,
    string? PortfolioId,
    string? Portfolio,
    string? Alias,
    string? Broker,
    string? StrategyId,
    string? SymbolId,
    string Symbol,
    string? InstrumentId,
    string? Reference,
    int? CoreOrderId,
    int? SubOrderId,
    string? Label,
    string? Side,
    decimal? OrderPrice,
    string? OrderId,
    DateTimeOffset? OrderTimestamp,
    decimal? OrderSize,
    string? OrderChannel,
    string? ProviderId,
    DateTimeOffset? ProviderTimestamp,
    DateTimeOffset? ExecuteTimestamp,
    decimal? EntitySize,
    decimal? ExecuteSize,
    decimal? ExecutePrice,
    DateTimeOffset? TradeTimestamp,
    string? Event,
    string? UserName,
    string? Code,
    string? RecordType,
    decimal? Quote,
    decimal? Amount,
    decimal? Rate,
    DateTime RecordedAtUtc);
