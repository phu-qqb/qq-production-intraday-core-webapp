using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TradingDaemon.Data;
using TradingDaemon.Models;

namespace TradingDaemon.Services;

public class WakettPriceFetcher
{
    private readonly WakettApiClient _client;
    private readonly DapperContext _context;
    private readonly IConfiguration _config;
    private readonly ILogger<WakettPriceFetcher> _logger;

    private readonly WakettAutomationOptions? _automationOptions;
    private readonly string _stageHistCloseTable;
    private readonly string _flatBarStagingTable;
    private readonly string _priceBarWindowQuery;
    private readonly string _priceBarSelectWithOffsetSql;
    private readonly string _stageDeleteSql;
    private readonly string _stageInsertSql;
    private readonly string _priceBarTable;


    private int PriceMinuteOffset => Math.Clamp(
        _config.GetValue<int?>("ExternalApis:WakettApi:PriceMinuteOffset")
            ?? 6,
        0,
        59);

    public WakettPriceFetcher(
        WakettApiClient client,
        DapperContext context,
        IConfiguration config,
        ILogger<WakettPriceFetcher> logger,
        IDatabaseObjectNameProvider databaseNameProvider,
        IOptions<WakettAutomationOptions>? automationOptions = null)
    {
        _client = client;
        _context = context;
        _config = config;
        _logger = logger;
        _automationOptions = automationOptions?.Value;
        _stageHistCloseTable = databaseNameProvider.GetObjectName(DatabaseObjects.IntradayMarketStageHistClose);
        _flatBarStagingTable = databaseNameProvider.GetObjectName(DatabaseObjects.IntradayStagingFlatBar);
        _priceBarTable = databaseNameProvider.GetObjectName(DatabaseObjects.IntradayMarketPriceBar);
        _priceBarWindowQuery = $"SELECT SecurityId, BarTimeUtc FROM {_priceBarTable} WHERE TimeframeMinute = 60 AND SecurityId IN @SecurityIds AND DATEPART(MINUTE, BarTimeUtc) = @MinuteOffset AND BarTimeUtc BETWEEN @StartUtc AND @EndUtc";
        _priceBarSelectWithOffsetSql = $"SELECT SecurityId, BarTimeUtc, [Close] FROM {_priceBarTable} WHERE TimeframeMinute = 60 AND SecurityId IN @SecurityIds AND DATEPART(MINUTE, BarTimeUtc) = @MinuteOffset";
        _stageDeleteSql = $"DELETE FROM {_stageHistCloseTable} WHERE BarTimeUtc = @BarTimeUtc AND SecurityId IN @SecurityIds";
        _stageInsertSql = $"INSERT INTO {_stageHistCloseTable} (SecurityId, BarTimeUtc, [Close]) VALUES (@SecurityId, @BarTimeUtc, @Close)";
    }

    public async Task<WakettPriceUploadResult?> FetchAndStoreAsync(
        CancellationToken cancellationToken = default)
    {
        if (!TryLoadConfiguredSymbols(out var baseSymbols, out var missingSymbols, out var allSecurityIds))
        {
            return null;
        }

        var missingBars = await FindMissingBarTimestampsAsync(allSecurityIds, cancellationToken);
        if (missingBars.Count == 0)
        {
            _logger.LogInformation("No missing Wakett price bars detected in the last 24 hours.");
            return null;
        }

        var securityPairs = await LoadSecurityPairsAsync(allSecurityIds, cancellationToken);
        var wakettSymbols = BuildWakettRequestSymbols(baseSymbols);
        WakettPriceUploadResult? lastResult = null;

        var nowUtc = DateTimeOffset.UtcNow;

        foreach (var barTimeUtc in missingBars.OrderBy(t => t))
        {

            var baseTimestamp = new DateTimeOffset(barTimeUtc, TimeSpan.Zero);
            var minuteOffset = PriceMinuteOffset;
            var requestTimestamp = baseTimestamp.AddMinutes(minuteOffset);
            var expectedBarTimestamp = baseTimestamp.AddMinutes(minuteOffset);

            if (requestTimestamp > nowUtc)
            {
                _logger.LogInformation(
                    "Skipping Wakett price request for timestamp {TimestampUtc} because it is ahead of the allowed window.",
                    requestTimestamp.UtcDateTime);
                continue;
            }

            _logger.LogInformation(
                "Requesting Wakett prices for timestamp {TimestampUtc}.",
                requestTimestamp.UtcDateTime);

                var response = await _client.GetPricesAsync(wakettSymbols, requestTimestamp);
            var computedRates = BuildComputedRates(baseSymbols, missingSymbols, response?.Prices, _logger);
            if (computedRates.Count == 0)
            {
                _logger.LogWarning(
                    "Unable to compute any FX rates from Wakett response for timestamp {TimestampUtc}.",
                    barTimeUtc);
                continue;
            }

            var timestampUtc = DetermineTimestampUtc(
                response?.Ts,
                expectedBarTimestamp,
                expectedBarTimestamp.UtcDateTime);
            _logger.LogInformation("Using timestamp {TimestampUtc} for Wakett price upload.", timestampUtc);

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
                _logger.LogWarning(
                    "No database records were produced after adjusting rates for timestamp {TimestampUtc}.",
                    barTimeUtc);
                continue;
            }

            await StoreAsync(dbRecords.Values, cancellationToken);

            var ordered = uploadItems.Values
                .OrderBy(i => i.SecurityId)
                .ToList();

            _logger.LogInformation(
                "Uploaded {Count} FX prices to Stage_HistClose for {TimestampUtc}.",
                ordered.Count,
                timestampUtc);

            lastResult = new WakettPriceUploadResult(timestampUtc, ordered);
        }

        return lastResult;
    }

    public async Task<bool> AreRecentPricesCompleteAsync(CancellationToken cancellationToken = default)
    {
        if (!TryLoadConfiguredSymbols(out _, out _, out var securityIds))
        {
            return false;
        }

        var missingBars = await FindMissingBarTimestampsAsync(securityIds, cancellationToken);
        if (missingBars.Count == 0)
        {
            _logger.LogInformation("All Wakett prices are present for the last 24 trading hours.");
            return true;
        }

        _logger.LogWarning(
            "Detected {Count} missing Wakett price bar(s) within the last 24 trading hours.",
            missingBars.Count);
        _logger.LogWarning(
            "Missing bar timestamps (UTC): {BarTimes}",
            string.Join(", ", missingBars
                .OrderBy(t => t)
                .Select(t => t.ToString("yyyy-MM-dd HH:mm"))));
        return false;
    }

    internal static IReadOnlyList<ComputedRate> BuildComputedRates(
        IEnumerable<WakettSecuritySymbol> baseSymbols,
        IEnumerable<WakettSecuritySymbol> missingSymbols,
        IEnumerable<WakettPrice>? prices,
        ILogger? logger)
    {
        var result = new List<ComputedRate>();
        var graph = new Dictionary<string, Dictionary<string, decimal>>(StringComparer.OrdinalIgnoreCase);

        var priceLookup = new Dictionary<string, (WakettPrice Price, bool Inverted)>(StringComparer.OrdinalIgnoreCase);
        if (prices is not null)
        {
            foreach (var p in prices)
            {
                var key = NormalizeSymbol(p.Symbol);
                if (string.IsNullOrEmpty(key))
                    continue;
                priceLookup[key] = (p, false);

                if (key.Length == 6)
                {
                    var reversed = key[3..] + key[..3];
                    if (!priceLookup.ContainsKey(reversed))
                    {
                        priceLookup[reversed] = (p, true);
                    }
                }
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
            if (!priceLookup.TryGetValue(lookupKey, out var entry))
            {
                logger?.LogWarning("No price returned from Wakett for symbol {Symbol}.", symbol.Symbol);
                continue;
            }

            if (entry.Price.Error is not null)
            {
                logger?.LogWarning(
                    "Wakett returned error for symbol {Symbol}: {Code} {Message}.",
                    symbol.Symbol,
                    entry.Price.Error.Code,
                    entry.Price.Error.Message);
                continue;
            }

            var mid = TryGetMidPrice(entry.Price);
            if (!mid.HasValue || mid.Value <= 0m)
            {
                logger?.LogWarning(
                    "Invalid price for symbol {Symbol}. Bid={Bid} Ask={Ask} Mid={Mid}",
                    symbol.Symbol,
                    entry.Price.Bid,
                    entry.Price.Ask,
                    entry.Price.Mid);
                continue;
            }

            var rateValue = entry.Inverted
                ? 1m / mid.Value
                : mid.Value;

            AddRate(graph, pair, rateValue);
            result.Add(new ComputedRate(symbol, pair, rateValue));
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

    internal static bool TryComputeCrossRate(IDictionary<string, Dictionary<string, decimal>> graph, CurrencyPair target, out decimal rate)
    {
        rate = 0m; 
        if (target.Base.Equals(target.Quote, StringComparison.OrdinalIgnoreCase)) { 
            rate = 1m; return true; 
        }
        if (!graph.ContainsKey(target.Base) || !graph.ContainsKey(target.Quote)) return false; 
        var queue = new Queue<(string Currency, decimal Rate)>(); 
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { target.Base }; 
        queue.Enqueue((target.Base, 1m)); 
        while (queue.Count > 0) 
        { 
            var (currency, currentRate) = queue.Dequeue(); 
            if (!graph.TryGetValue(currency, out var edges)) continue; 
            foreach (var kvp in edges) { if (!visited.Add(kvp.Key)) continue; 
                var nextRate = currentRate * kvp.Value; 
                if (kvp.Key.Equals(target.Quote, StringComparison.OrdinalIgnoreCase)) { 
                    rate = nextRate; return true; 
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

    private bool TryLoadConfiguredSymbols(
        out IReadOnlyList<WakettSecuritySymbol> baseSymbols,
        out IReadOnlyList<WakettSecuritySymbol> missingSymbols,
        out int[] allSecurityIds)
    {
        baseSymbols = _config
            .GetSection("ExternalApis:WakettApi:Symbols")
            .Get<List<WakettSecuritySymbol>>() ?? new();

        if (baseSymbols.Count == 0)
        {
            _logger.LogWarning("No Wakett symbols configured. Aborting Wakett price processing.");
            missingSymbols = Array.Empty<WakettSecuritySymbol>();
            allSecurityIds = Array.Empty<int>();
            return false;
        }

        missingSymbols = _config
            .GetSection("ExternalApis:WakettApi:MissingSymbols")
            .Get<List<WakettSecuritySymbol>>() ?? new();

        allSecurityIds = baseSymbols
            .Concat(missingSymbols)
            .Select(s => s.SecurityId)
            .Distinct()
            .ToArray();

        return true;
    }

    private static IReadOnlyList<WakettSecuritySymbol> BuildWakettRequestSymbols(
        IReadOnlyList<WakettSecuritySymbol> configuredSymbols)
    {
        if (configuredSymbols.Count == 0)
        {
            return Array.Empty<WakettSecuritySymbol>();
        }

        var result = new List<WakettSecuritySymbol>(configuredSymbols.Count);

        foreach (var symbol in configuredSymbols)
        {
            var requestSymbol = WakettSymbolPatch.GetRequestSymbol(symbol.SecurityId, symbol.Symbol);

            if (string.IsNullOrWhiteSpace(requestSymbol))
            {
                continue;
            }

            result.Add(new WakettSecuritySymbol
            {
                SecurityId = symbol.SecurityId,
                Symbol = requestSymbol
            });
        }

        return result;
    }

    private async Task<IReadOnlyList<DateTime>> FindMissingBarTimestampsAsync(
        IReadOnlyCollection<int> securityIds,
        CancellationToken cancellationToken)
    {
        if (securityIds.Count == 0)
            return Array.Empty<DateTime>();

        var nowUtc = DateTime.UtcNow;
        var normalizedNowUtc = NormalizeToHourUtc(nowUtc);
        var expectedTimestamps = BuildExpectedBarHours(normalizedNowUtc, 24);
        if (expectedTimestamps.Count == 0)
        {
            return Array.Empty<DateTime>();
        }

        var minuteOffset = PriceMinuteOffset;
        var startHourUtc = expectedTimestamps[0];
        var endHourUtc = expectedTimestamps[^1];
        var startUtc = startHourUtc.AddMinutes(minuteOffset);
        var endUtc = endHourUtc.AddMinutes(minuteOffset);

        using var connection = _context.CreateConnection();
        if (connection is DbConnection dbConnection)
        {
            await dbConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            connection.Open();
        }

        var rows = await connection.QueryAsync<(int SecurityId, DateTime BarTimeUtc)>(
        _priceBarWindowQuery,
        new
        {
            SecurityIds = securityIds.ToArray(),
            StartUtc = startUtc,
            EndUtc = endUtc,
            MinuteOffset = minuteOffset
        });

        var existing = new Dictionary<DateTime, HashSet<int>>();
        foreach (var row in rows)
        {
            var normalized = DateTime.SpecifyKind(row.BarTimeUtc, DateTimeKind.Utc);
            normalized = new DateTime(
                normalized.Year,
                normalized.Month,
                normalized.Day,
                normalized.Hour,
                0,
                0,
                DateTimeKind.Utc);

            if (!existing.TryGetValue(normalized, out var set))
            {
                set = new HashSet<int>();
                existing[normalized] = set;
            }

            set.Add(row.SecurityId);
        }

        var missing = new List<DateTime>();
        foreach (var timestamp in expectedTimestamps)
        {
            if (!existing.TryGetValue(timestamp, out var set) || set.Count < securityIds.Count)
            {
                missing.Add(timestamp);
            }
        }

        return missing;
    }

    internal static IReadOnlyList<DateTime> BuildExpectedBarHours(DateTime endHourUtc, int count)
    {
        if (count <= 0)
        {
            return Array.Empty<DateTime>();
        }

        var normalizedEnd = NormalizeToHourUtc(endHourUtc);
        var result = new List<DateTime>(count);

        var current = normalizedEnd;
        while (result.Count < count)
        {
            if (!IsWeekend(current))
            {
                result.Add(current);
            }

            current = current.AddHours(-1);
        }

        result.Reverse();
        return result;
    }

    private static DateTime NormalizeToHourUtc(DateTime value)
    {
        var utc = value.Kind == DateTimeKind.Utc
            ? value
            : DateTime.SpecifyKind(value, DateTimeKind.Utc);

        return new DateTime(
            utc.Year,
            utc.Month,
            utc.Day,
            utc.Hour,
            0,
            0,
            DateTimeKind.Utc);
    }

    private static bool IsWeekend(DateTime value)
    {
        var utc = value.Kind == DateTimeKind.Utc
            ? value
            : DateTime.SpecifyKind(value, DateTimeKind.Utc);

        var local = TimeZoneInfo.ConvertTimeFromUtc(utc, NewYorkZone);
        return local.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday;
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

        var securityKeys = recordList
            .Select(r => r.SecurityKey)
            .Distinct()
            .ToArray();

        using var connection = _context.CreateConnection();
        if (connection is DbConnection dbConnection)
        {
            await dbConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            connection.Open();
        }

        IDbTransaction? transaction = null;

        try
        {
            if (recordList.Count > 0)
            {
                transaction = connection.BeginTransaction();
                var parameters = new
                {
                    BarTimeUtc = recordList[0].BarTimeUtc,
                    SecurityIds = securityKeys
                };

                await connection.ExecuteAsync(_stageDeleteSql, parameters, transaction);
                await connection.ExecuteAsync(
                    _stageInsertSql,
                    recordList.Select(r => new { SecurityId = r.SecurityKey, r.BarTimeUtc, Close = r.Close }),
                    transaction);

                transaction.Commit();
            }
        }
        finally
        {
            transaction?.Dispose();
        }

        if (recordList.Count > 0)
        {
            await PriceProcessingProcedures.LoadRawFromStageAsync(connection, 60, cancellationToken);

            var minuteOffset = PriceMinuteOffset;
            var selectRaw = _priceBarSelectWithOffsetSql;
            var existing = (await connection.QueryAsync<HistClose>(selectRaw, new { SecurityIds = securityKeys, MinuteOffset = minuteOffset }))
                .GroupBy(r => (r.SecurityId, r.BarTimeUtc))
                .Select(g => g.Last())
                .ToList();

            var flatRecords = new List<FlatPrice>();
            foreach (var grp in existing.GroupBy(r => r.SecurityId))
            {
                var ordered = grp.OrderBy(r => r.BarTimeUtc).ToList();
                var rawEu = RawNMin(ordered, 60, "EU", minuteOffset);
                var flatEu = Flatten(rawEu, SessionBounds["EU"].Zone)
                    .Select(r => new FlatPrice
                    {
                        SecurityId = grp.Key,
                        BarTimeUtc = r.TimestampUtc,
                        Close = r.Close,
                        Session = "EU"
                    });
                flatRecords.AddRange(flatEu);

                var rawUs = RawNMin(ordered, 60, "US", minuteOffset);
                var flatUs = Flatten(rawUs, SessionBounds["US"].Zone)
                    .Select(r => new FlatPrice
                    {
                        SecurityId = grp.Key,
                        BarTimeUtc = r.TimestampUtc,
                        Close = r.Close,
                        Session = "US"
                    });
                flatRecords.AddRange(flatUs);
            }

            if (flatRecords.Count > 0)
            {
                await connection.ExecuteAsync($"DELETE FROM {_flatBarStagingTable}");

                var table = new DataTable();
                table.Columns.Add("SecurityId", typeof(string));
                table.Columns.Add("BarTimeUtc", typeof(DateTime));
                table.Columns.Add("Close", typeof(decimal));
                table.Columns.Add("Session", typeof(string));

                foreach (var record in flatRecords)
                {
                    table.Rows.Add(record.SecurityId, record.BarTimeUtc, record.Close, record.Session);
                }

                if (connection is SqlConnection sqlConnection)
                {
                    using var bulkCopy = new SqlBulkCopy(sqlConnection);
                    bulkCopy.DestinationTableName = _flatBarStagingTable;
                    await bulkCopy.WriteToServerAsync(table);
                }
                else
                {
                    throw new InvalidOperationException("Expected SqlConnection for bulk copy operations.");
                }
            }

            await PriceProcessingProcedures.LoadFlatFromMinimalAsync(connection, 60, cancellationToken);
        }
    }

    private static readonly Dictionary<string, (TimeZoneInfo Zone, TimeSpan Start, TimeSpan End)> SessionBounds = new()
    {
        ["US"] = (NewYorkZone, TimeSpan.Parse("09:00"), TimeSpan.Parse("15:59")),
        ["EU"] = (NewYorkZone, TimeSpan.Parse("02:00"), TimeSpan.Parse("08:59"))
    };

    private static TimeZoneInfo NewYorkZone => TimeZoneInfo.FindSystemTimeZoneById(
        RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Eastern Standard Time" : "America/New_York");

    private static List<(DateTime TimestampUtc, decimal Close)> RawNMin(List<HistClose> series, int minutes, string session, int offset)
    {
        var bounds = SessionBounds[session];
        var zone = bounds.Zone;
        var result = new List<(DateTime, decimal)>();
        DateTime? currentBucket = null;
        decimal lastClose = 0;
        var sessionStartAligned = AlignSessionStart(bounds.Start, minutes);
        foreach (var item in series.OrderBy(s => s.BarTimeUtc))
        {
            var local = TimeZoneInfo.ConvertTimeFromUtc(item.BarTimeUtc, zone);
            if (offset != 0) local = local.AddMinutes(-offset);
            var start = local.TimeOfDay;
            var end = start.Add(TimeSpan.FromMinutes(minutes - 1));

            if (start < bounds.Start || end > bounds.End) continue;
            var bucket = AlignToSessionBucket(local, sessionStartAligned, minutes);

            if (currentBucket != bucket)
            {
                if (currentBucket.HasValue)
                    result.Add((TimeZoneInfo.ConvertTimeToUtc(currentBucket.Value.AddMinutes(offset), zone), lastClose));
                currentBucket = bucket;
            }
            lastClose = item.Close;
        }
        if (currentBucket.HasValue)
            result.Add((TimeZoneInfo.ConvertTimeToUtc(currentBucket.Value.AddMinutes(offset), zone), lastClose));
        return result;
    }

    private static DateTime AlignToSessionBucket(DateTime local, TimeSpan sessionStartAligned, int minutes)
    {
        var alignedDayStart = new DateTime(local.Year, local.Month, local.Day).Add(sessionStartAligned);
        if (local.TimeOfDay <= sessionStartAligned)
        {
            return alignedDayStart;
        }

        var minutesSinceAlignedStart = (int)Math.Floor((local.TimeOfDay - sessionStartAligned).TotalMinutes / minutes) * minutes;
        return alignedDayStart.AddMinutes(minutesSinceAlignedStart);
    }


    private static TimeSpan AlignSessionStart(TimeSpan sessionStart, int minutes)
    {
        if (minutes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(minutes), "Aggregation interval must be positive.");
        }

        if (sessionStart == TimeSpan.Zero)
        {
            return TimeSpan.Zero;
        }

        var totalMinutes = (int)Math.Ceiling(sessionStart.TotalMinutes / minutes) * minutes;
        return TimeSpan.FromMinutes(totalMinutes);
    }

    private static List<(DateTime TimestampUtc, decimal Close)> Flatten(List<(DateTime TimestampUtc, decimal Close)> raw, TimeZoneInfo zone)
    {
        if (raw.Count == 0) return new();
        var times = raw.Select(r => r.TimestampUtc).ToList();
        var px = raw.Select(r => r.Close).ToList();
        var localTimes = times.Select(t => TimeZoneInfo.ConvertTimeFromUtc(t, zone)).ToList();
        var ret = new decimal[px.Count];
        for (int i = 1; i < px.Count; i++)
        {
            var prev = px[i - 1];
            ret[i] = prev != 0 ? (px[i] - prev) / prev : 0m;
            if (localTimes[i].Date != localTimes[i - 1].Date)
                ret[i] = 0m;
        }
        var flat = new decimal[px.Count];
        flat[px.Count - 1] = px[px.Count - 1];
        for (int i = px.Count - 2; i >= 0; i--)
        {
            var inc = ret[i + 1];
            flat[i] = flat[i + 1] / (1 + inc);
        }
        var result = new List<(DateTime, decimal)>();
        for (int i = 0; i < px.Count; i++)
            result.Add((times[i], flat[i]));
        return result;
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
