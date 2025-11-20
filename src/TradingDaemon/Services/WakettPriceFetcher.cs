using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
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
using TradingDaemon.Options;

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
    private readonly string _priceBarTimestampPresenceSql;
    private readonly string _stageClearSql;
    private readonly string _stageDeleteSql;
    private readonly string _flatStageClearSql;
    private readonly string _stageInsertSql;
    private readonly string _priceBarTable;
    private readonly string _securityTable;
    private readonly IPriceProcessingProcedureExecutor _priceProcedures;
    private readonly PriceBarOptions _priceBarOptions;


    private IReadOnlyList<int>? _priceMinuteOffsets;

    private static readonly TimeSpan HistoricalWindow = TimeSpan.FromHours(6);

    private IReadOnlyList<int> PriceMinuteOffsets => _priceMinuteOffsets ??= LoadMinuteOffsets();

    public WakettPriceFetcher(
        WakettApiClient client,
        DapperContext context,
        IConfiguration config,
        ILogger<WakettPriceFetcher> logger,
        IDatabaseObjectNameProvider databaseNameProvider,
        IPriceProcessingProcedureExecutor priceProcedures,
        IOptions<WakettAutomationOptions>? automationOptions = null,
        IOptions<PriceBarOptions>? priceBarOptions = null)
    {
        _client = client;
        _context = context;
        _config = config;
        _logger = logger;
        _priceProcedures = priceProcedures;
        _automationOptions = automationOptions?.Value;
        _priceBarOptions = priceBarOptions?.Value ?? new PriceBarOptions();
        _stageHistCloseTable = databaseNameProvider.GetObjectName(DatabaseObjects.IntradayMarketStageHistClose);
        _flatBarStagingTable = databaseNameProvider.GetObjectName(DatabaseObjects.IntradayStagingFlatBar);
        _priceBarTable = databaseNameProvider.GetObjectName(DatabaseObjects.IntradayMarketPriceBar);
        _securityTable = databaseNameProvider.GetObjectName(DatabaseObjects.IntradayCoreSecurity);
        _priceBarWindowQuery = $"SELECT SecurityId, BarTimeUtc FROM {_priceBarTable} WHERE TimeframeMinute = {PriceTimeframeMinute} AND SecurityId IN @SecurityIds AND DATEPART(MINUTE, BarTimeUtc) = @MinuteOffset AND BarTimeUtc BETWEEN @StartUtc AND @EndUtc";
        _priceBarSelectWithOffsetSql = $"SELECT SecurityId, BarTimeUtc, [Close] FROM {_priceBarTable} WHERE TimeframeMinute = {PriceTimeframeMinute} AND SecurityId IN @SecurityIds";
        _priceBarTimestampPresenceSql = $"SELECT SecurityId FROM {_priceBarTable} WHERE TimeframeMinute = {PriceTimeframeMinute} AND SecurityId IN @SecurityIds AND BarTimeUtc = @BarTimeUtc";
        _stageClearSql = $"DELETE FROM {_stageHistCloseTable}";
        _stageDeleteSql = $"DELETE FROM {_stageHistCloseTable} WHERE BarTimeUtc = @BarTimeUtc AND SecurityId IN @SecurityIds";
        _flatStageClearSql = $"DELETE FROM {_flatBarStagingTable}";
        _stageInsertSql = $"INSERT INTO {_stageHistCloseTable} (SecurityId, BarTimeUtc, [Close]) VALUES (@SecurityId, @BarTimeUtc, @Close)";
    }

    private IReadOnlyList<int> LoadMinuteOffsets()
    {
        var configuredOffsets = _config
            .GetSection("ExternalApis:WakettApi:PriceMinuteOffsets")
            .Get<int[]>()
            ?.Select(offset => Math.Clamp(offset, 0, 59))
            .Distinct()
            .OrderBy(offset => offset)
            .ToArray();

        if (configuredOffsets is { Length: > 0 })
        {
            return configuredOffsets;
        }

        var fallback = _config.GetValue<int?>("ExternalApis:WakettApi:PriceMinuteOffset") ?? 6;
        return new[] { Math.Clamp(fallback, 0, 59) };
    }

    public async Task<WakettPriceUploadResult?> FetchAndStoreAsync(
        CancellationToken cancellationToken = default)
    {
        var symbolConfiguration = await LoadSymbolConfigurationAsync(cancellationToken);
        if (symbolConfiguration is null)
        {
            return null;
        }

        var fetchStopwatch = Stopwatch.StartNew();
        var baseSymbols = symbolConfiguration.BaseSymbols;
        var missingSymbols = symbolConfiguration.MissingSymbols;
        var allSecurityIds = symbolConfiguration.AllSecurityIds;
        var uploadSecurityIds = symbolConfiguration.AllSecurityIds;
        var uploadSecurityIdSet = new HashSet<int>(uploadSecurityIds);

        if (uploadSecurityIds.Length == 0)
        {
            _logger.LogInformation("No Wakett securities require database uploads.");
            return null;
        }

        using var connection = await OpenConnectionAsync(cancellationToken);

        _logger.LogInformation("[Wakett] Beginning fetch cycle for {Count} securities.", uploadSecurityIds.Length);

        var stageStopwatch = Stopwatch.StartNew();
        await ClearStageTablesAsync(connection, cancellationToken);
        stageStopwatch.Stop();
        _logger.LogInformation("[Wakett] Cleared staging tables in {ElapsedMs} ms.", stageStopwatch.ElapsedMilliseconds);

        var missingBars = new List<(int MinuteOffset, DateTime BarTimeUtc)>();
        var missingDetection = Stopwatch.StartNew();
        foreach (var minuteOffset in PriceMinuteOffsets)
        {
            var offsetTimer = Stopwatch.StartNew();
            var missingForOffset = await FindMissingBarTimestampsAsync(
                uploadSecurityIds,
                minuteOffset,
                connection,
                cancellationToken);
            missingBars.AddRange(missingForOffset.Select(bar => (minuteOffset, bar)));
            offsetTimer.Stop();
            _logger.LogInformation(
                "[Wakett] Missing scan for offset {Offset} found {MissingCount} gaps in {ElapsedMs} ms.",
                minuteOffset,
                missingForOffset.Count,
                offsetTimer.ElapsedMilliseconds);
        }
        missingDetection.Stop();
        _logger.LogInformation(
            "[Wakett] Completed missing-bar discovery across {OffsetCount} offsets in {ElapsedMs} ms.",
            PriceMinuteOffsets.Count,
            missingDetection.ElapsedMilliseconds);

        var nowUtc = DateTimeOffset.UtcNow;
        var historicalWindowStart = nowUtc.Subtract(HistoricalWindow);

        var fetchableMissingBars = missingBars
            .Where(entry => entry.BarTimeUtc.AddMinutes(entry.MinuteOffset) >= historicalWindowStart.UtcDateTime)
            .ToList();

        if (fetchableMissingBars.Count == 0)
        {
            _logger.LogInformation(
                "No missing Wakett price bars detected within the last {Hours} hours for configured minute offsets.",
                HistoricalWindow.TotalHours);
            return null;
        }

        var loadPairsTimer = Stopwatch.StartNew();
        var securityPairs = await LoadSecurityPairsAsync(allSecurityIds, connection, cancellationToken);
        loadPairsTimer.Stop();
        _logger.LogInformation(
            "[Wakett] Loaded {Count} security pairs in {ElapsedMs} ms.",
            securityPairs.Count,
            loadPairsTimer.ElapsedMilliseconds);
        var wakettSymbols = BuildWakettRequestSymbols(baseSymbols);
        WakettPriceUploadResult? lastResult = null;

        foreach (var (minuteOffset, barTimeUtc) in fetchableMissingBars
            .OrderBy(entry => entry.BarTimeUtc.AddMinutes(entry.MinuteOffset)))
        {
            var loopTimer = Stopwatch.StartNew();

            var baseTimestamp = new DateTimeOffset(barTimeUtc, TimeSpan.Zero);
            var requestTimestamp = baseTimestamp.AddMinutes(minuteOffset);
            var expectedBarTimestamp = baseTimestamp.AddMinutes(minuteOffset);
            var expectedBarTimestampUtc = DateTime.SpecifyKind(expectedBarTimestamp.UtcDateTime, DateTimeKind.Utc);

            if (requestTimestamp > nowUtc)
            {
                _logger.LogInformation(
                    "Skipping Wakett price request for timestamp {TimestampUtc} because it is ahead of the allowed window.",
                    requestTimestamp.UtcDateTime);
                continue;
            }

            if (requestTimestamp < historicalWindowStart)
            {
                _logger.LogInformation(
                    "Skipping Wakett price request for timestamp {TimestampUtc} because it is outside the 6-hour history window.",
                    requestTimestamp.UtcDateTime);
                continue;
            }

            var presenceTimer = Stopwatch.StartNew();
            if (!await ArePricesMissingForTimestampAsync(
                uploadSecurityIds,
                expectedBarTimestampUtc,
                connection,
                cancellationToken))
            {
                _logger.LogInformation(
                    "Skipping Wakett price request for timestamp {TimestampUtc} because all price bars already exist.",
                    expectedBarTimestampUtc);
                continue;
            }
            presenceTimer.Stop();
            _logger.LogInformation(
                "[Wakett] Presence check for {TimestampUtc} took {ElapsedMs} ms.",
                expectedBarTimestampUtc,
                presenceTimer.ElapsedMilliseconds);

            _logger.LogInformation(
                "Requesting Wakett prices for timestamp {TimestampUtc}.",
                requestTimestamp.UtcDateTime);

            var apiTimer = Stopwatch.StartNew();
            var response = await _client.GetPricesAsync(wakettSymbols, requestTimestamp);
            apiTimer.Stop();
            _logger.LogInformation(
                "[Wakett] Wakett API call for {TimestampUtc} returned in {ElapsedMs} ms.",
                requestTimestamp.UtcDateTime,
                apiTimer.ElapsedMilliseconds);
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
                if (!uploadSecurityIdSet.Contains(rate.Definition.SecurityId))
                {
                    continue;
                }

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

            var storeTimer = Stopwatch.StartNew();
            await StoreAsync(connection, dbRecords.Values, minuteOffset, cancellationToken);
            storeTimer.Stop();
            _logger.LogInformation(
                "[Wakett] Stored {Count} records for {TimestampUtc} in {ElapsedMs} ms.",
                dbRecords.Count,
                timestampUtc,
                storeTimer.ElapsedMilliseconds);

            var ordered = uploadItems.Values
                .OrderBy(i => i.SecurityId)
                .ToList();

            _logger.LogInformation(
                "Uploaded {Count} FX prices to Stage_HistClose for {TimestampUtc}.",
                ordered.Count,
                timestampUtc);

            lastResult = new WakettPriceUploadResult(timestampUtc, ordered);
            loopTimer.Stop();
            _logger.LogInformation(
                "[Wakett] End-to-end handling for {TimestampUtc} completed in {ElapsedMs} ms.",
                timestampUtc,
                loopTimer.ElapsedMilliseconds);
        }

        fetchStopwatch.Stop();
        _logger.LogInformation("[Wakett] Fetch cycle completed in {ElapsedMs} ms.", fetchStopwatch.ElapsedMilliseconds);
        return lastResult;
    }

    public async Task<bool> AreRecentPricesCompleteAsync(CancellationToken cancellationToken = default)
    {
        var symbolConfiguration = await LoadSymbolConfigurationAsync(cancellationToken);
        if (symbolConfiguration is null)
        {
            return false;
        }

        var securityIds = symbolConfiguration.AllSecurityIds;
        var uploadSecurityIds = symbolConfiguration.AllSecurityIds;

        if (uploadSecurityIds.Length == 0)
        {
            _logger.LogInformation("No Wakett securities require database uploads.");
            return true;
        }

        var nowUtc = DateTimeOffset.UtcNow;
        var historicalWindowStart = nowUtc.Subtract(HistoricalWindow).UtcDateTime;

        var missingByOffset = new List<(int MinuteOffset, IReadOnlyList<DateTime> Missing)>();
        using var connection = await OpenConnectionAsync(cancellationToken);

        foreach (var minuteOffset in PriceMinuteOffsets)
        {
            var missingBars = await FindMissingBarTimestampsAsync(
                uploadSecurityIds,
                minuteOffset,
                connection,
                cancellationToken);
            var relevantMissing = missingBars
                .Where(bar => bar.AddMinutes(minuteOffset) >= historicalWindowStart)
                .ToList();

            if (relevantMissing.Count > 0)
            {
                missingByOffset.Add((minuteOffset, relevantMissing));
            }
        }

        if (missingByOffset.Count == 0)
        {
            _logger.LogInformation("All Wakett prices are present for the last 24 trading hours.");
            return true;
        }

        foreach (var (minuteOffset, missingBars) in missingByOffset)
        {
            _logger.LogWarning(
                "Detected {Count} missing Wakett price bar(s) at minute offset {MinuteOffset} within the last 24 trading hours.",
                missingBars.Count,
                minuteOffset);
            _logger.LogWarning(
                "Missing bar timestamps (UTC): {BarTimes}",
                string.Join(
                    ", ",
                    missingBars
                        .OrderBy(t => t)
                        .Select(t => t.ToString("yyyy-MM-dd HH:mm"))));
        }

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

    private async Task<SymbolConfiguration?> LoadSymbolConfigurationAsync(CancellationToken cancellationToken)
    {
        var basePairs = LoadConfiguredBasePairs();
        if (basePairs.Count == 0)
        {
            _logger.LogWarning("No Wakett base pairs configured. Aborting Wakett price processing.");
            return null;
        }

        var allowedCurrencies = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var pair in basePairs)
        {
            allowedCurrencies.Add(pair.Base);
            allowedCurrencies.Add(pair.Quote);
        }

        var securityDefinitions = await LoadSecurityDefinitionsAsync(allowedCurrencies, cancellationToken);
        if (securityDefinitions.Count == 0)
        {
            _logger.LogWarning(
                "No FX securities were found in {Table} for the configured Wakett currencies.",
                _securityTable);
            return null;
        }

        var baseSymbols = new List<WakettSecuritySymbol>();
        var baseSecurityIds = new HashSet<int>();

        foreach (var pair in basePairs)
        {
            var match = securityDefinitions.FirstOrDefault(def => PairMatches(def.Pair, pair));
            if (match is null)
            {
                _logger.LogWarning(
                    "Unable to locate security for Wakett base pair {Base}/{Quote}.",
                    pair.Base,
                    pair.Quote);
                continue;
            }

            baseSecurityIds.Add(match.SecurityId);
            baseSymbols.Add(new WakettSecuritySymbol
            {
                SecurityId = match.SecurityId,
                Symbol = FormatCurrencyPair(pair)
            });
        }

        if (baseSymbols.Count == 0)
        {
            _logger.LogWarning("None of the configured Wakett base pairs are present in the security table.");
            return null;
        }

        var missingSymbols = securityDefinitions
            .Where(def => !baseSecurityIds.Contains(def.SecurityId))
            .Select(def => new WakettSecuritySymbol
            {
                SecurityId = def.SecurityId,
                Symbol = FormatCurrencyPair(def.Pair)
            })
            .ToList();

        var allSecurityIds = baseSecurityIds
            .Concat(missingSymbols.Select(symbol => symbol.SecurityId))
            .Distinct()
            .ToArray();

        return new SymbolConfiguration(baseSymbols, missingSymbols, allSecurityIds);
    }

    private IReadOnlyList<CurrencyPair> LoadConfiguredBasePairs()
    {
        var configured = _config
            .GetSection("ExternalApis:WakettApi:BasePairs")
            .Get<string[]>() ?? Array.Empty<string>();

        var pairs = new List<CurrencyPair>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var entry in configured)
        {
            if (!TryParsePair(entry, out var pair))
            {
                _logger.LogWarning(
                    "Skipping invalid Wakett base pair configuration value '{Value}'.",
                    entry);
                continue;
            }

            var key = FormatCurrencyPair(pair);
            if (!seen.Add(key))
            {
                continue;
            }

            pairs.Add(pair);
        }

        return pairs;
    }

    private async Task<List<SecuritySymbolDefinition>> LoadSecurityDefinitionsAsync(
        ISet<string> allowedCurrencies,
        CancellationToken cancellationToken)
    {
        if (allowedCurrencies.Count == 0)
        {
            return new List<SecuritySymbolDefinition>();
        }

        var sql = $@"SELECT SecurityId, Symbol
FROM {_securityTable}
WHERE IsActive = 1 AND Symbol IS NOT NULL AND LTRIM(RTRIM(Symbol)) <> ''";

        using var connection = _context.CreateConnection();
        if (connection is DbConnection dbConnection)
        {
            await dbConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            connection.Open();
        }

        var definition = new CommandDefinition(sql, cancellationToken: cancellationToken);
        var rows = await connection.QueryAsync<SecuritySymbolRow>(definition);
        var definitions = new List<SecuritySymbolDefinition>();

        foreach (var row in rows)
        {
            if (string.IsNullOrWhiteSpace(row.Symbol))
            {
                continue;
            }

            var normalized = NormalizeSymbol(row.Symbol);
            if (normalized.Length != 6)
            {
                continue;
            }

            var pair = new CurrencyPair(normalized[..3], normalized.Substring(3, 3));
            if (!allowedCurrencies.Contains(pair.Base) || !allowedCurrencies.Contains(pair.Quote))
            {
                continue;
            }

            definitions.Add(new SecuritySymbolDefinition(row.SecurityId, pair));
        }

        return definitions;
    }

    private static string FormatCurrencyPair(CurrencyPair pair) => $"{pair.Base}/{pair.Quote}";

    private static bool PairMatches(CurrencyPair candidate, CurrencyPair target)
    {
        if (candidate.Equals(target))
        {
            return true;
        }

        return candidate.Base.Equals(target.Quote, StringComparison.OrdinalIgnoreCase)
            && candidate.Quote.Equals(target.Base, StringComparison.OrdinalIgnoreCase);
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
        int minuteOffset,
        IDbConnection? connection,
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

        var startHourUtc = expectedTimestamps[0];
        var endHourUtc = expectedTimestamps[^1];
        var startUtc = startHourUtc.AddMinutes(minuteOffset);
        var endUtc = endHourUtc.AddMinutes(minuteOffset);

        var ownsConnection = connection is null;
        connection ??= await OpenConnectionAsync(cancellationToken);

        try
        {
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
        finally
        {
            if (ownsConnection)
            {
                connection.Dispose();
            }
        }
    }

    private async Task<bool> ArePricesMissingForTimestampAsync(
        IReadOnlyCollection<int> securityIds,
        DateTime expectedBarTimestampUtc,
        IDbConnection? connection,
        CancellationToken cancellationToken)
    {
        if (securityIds.Count == 0)
        {
            return false;
        }

        var ids = securityIds.ToArray();
        var ownsConnection = connection is null;
        connection ??= await OpenConnectionAsync(cancellationToken);

        try
        {
            var rows = await connection.QueryAsync<int>(
                _priceBarTimestampPresenceSql,
                new
                {
                    SecurityIds = ids,
                    BarTimeUtc = expectedBarTimestampUtc
                });

            var present = new HashSet<int>();
            foreach (var securityId in rows)
            {
                present.Add(securityId);
                if (present.Count == ids.Length)
                {
                    return false;
                }
            }

            return true;
        }
        finally
        {
            if (ownsConnection)
            {
                connection.Dispose();
            }
        }
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
        IDbConnection? connection,
        CancellationToken cancellationToken)
    {
        var ids = securityIds.Distinct().ToArray();
        if (ids.Length == 0)
            return new Dictionary<int, CurrencyPair>();

        var sql = $"SELECT SecurityId, BloombergTicker FROM {_securityTable} WHERE SecurityId IN @Ids";
        var ownsConnection = connection is null;
        connection ??= await OpenConnectionAsync(cancellationToken);

        var pairs = new Dictionary<int, CurrencyPair>();
        try
        {
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
        finally
        {
            if (ownsConnection)
            {
                connection.Dispose();
            }
        }
    }

    private async Task StoreAsync(
        IDbConnection connection,
        IEnumerable<DbPriceRecord> records,
        int minuteOffset,
        CancellationToken cancellationToken)
    {
        var recordList = records.ToList();

        var securityKeys = recordList
            .Select(r => r.SecurityKey)
            .Distinct()
            .ToArray();

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
            await _priceProcedures.LoadRawFromStageAsync(
                connection,
                PriceTimeframeMinute,
                _priceBarOptions.SourceId,
                cancellationToken);

            var selectRaw = _priceBarSelectWithOffsetSql;
            var existing = (await connection.QueryAsync<HistClose>(selectRaw, new { SecurityIds = securityKeys }))
                .GroupBy(r => (r.SecurityId, r.BarTimeUtc))
                .Select(g => g.Last())
                .ToList();

            var seriesBySecurity = existing
                .GroupBy(r => r.SecurityId)
                .Select(g => new { SecurityId = g.Key, Series = g.OrderBy(r => r.BarTimeUtc).ToList() })
                .ToList();

            var flatBarBuilds = FlatBarBuildSpecificationFactory.CreateDefault(PriceTimeframeMinute, minuteOffset);

            foreach (var build in flatBarBuilds)
            {
                var flatRecords = new List<FlatPrice>();
                foreach (var grp in seriesBySecurity)
                {
                    foreach (var session in new[] { "EU", "US", "EUUS" })
                    {
                        var raw = RawNMin(grp.Series, build.TimeframeMinute, session, build.OffsetMinute);
                        var flat = Flatten(raw, SessionBounds[session].Zone)
                            .Select(r => new FlatPrice
                            {
                                SecurityId = grp.SecurityId,
                                BarTimeUtc = r.TimestampUtc,
                                Close = r.Close,
                                Session = session
                            });
                        flatRecords.AddRange(flat);
                    }
                }

                if (flatRecords.Count == 0)
                {
                    continue;
                }

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

                await _priceProcedures.LoadFlatFromMinimalAsync(connection, build.TimeframeMinute, cancellationToken);
            }
        }
    }

    private async Task<IDbConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = _context.CreateConnection();
        if (connection is DbConnection dbConnection)
        {
            await dbConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            connection.Open();
        }

        return connection;
    }

    private async Task ClearStageTablesAsync(IDbConnection connection, CancellationToken cancellationToken)
    {
        await connection.ExecuteAsync(_stageClearSql);
        await connection.ExecuteAsync(_flatStageClearSql);
    }

    private int PriceTimeframeMinute => Math.Max(1, _priceBarOptions.TimeframeMinute);


    private static readonly Dictionary<string, (TimeZoneInfo Zone, TimeSpan Start, TimeSpan End)> SessionBounds = new()
    {
        ["US"] = (NewYorkZone, TimeSpan.Parse("09:00"), TimeSpan.Parse("15:59")),
        ["EU"] = (NewYorkZone, TimeSpan.Parse("02:00"), TimeSpan.Parse("08:59")),
        ["EUUS"] = (NewYorkZone, TimeSpan.Parse("02:00"), TimeSpan.Parse("11:59"))
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

        if (sessionStart < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(sessionStart), "Session start cannot be negative.");
        }

        return sessionStart;
    }

    private static List<(DateTime TimestampUtc, decimal Close)> Flatten(List<(DateTime TimestampUtc, decimal Close)> raw, TimeZoneInfo zone)
    {
        if (raw.Count == 0)
        {
            return new();
        }

        var ordered = raw
            .OrderBy(r => r.TimestampUtc)
            .ToList();
        var count = ordered.Count;

        var localDates = ordered
            .Select(r => TimeZoneInfo.ConvertTimeFromUtc(r.TimestampUtc, zone).Date)
            .ToArray();

        var returns = new decimal[count];
        for (var i = 1; i < count; i++)
        {
            var prevClose = ordered[i - 1].Close;
            returns[i] = prevClose != 0 ? (ordered[i].Close / prevClose) - 1m : 0m;
        }

        for (var i = 1; i < count; i++)
        {
            if (localDates[i] != localDates[i - 1])
            {
                returns[i] = 0m;
            }
        }

        var flattenedCloses = new decimal[count];
        flattenedCloses[count - 1] = ordered[count - 1].Close;
        for (var i = count - 2; i >= 0; i--)
        {
            var inc = returns[i + 1];
            flattenedCloses[i] = flattenedCloses[i + 1] / (1 + inc);
        }

        var result = new List<(DateTime TimestampUtc, decimal Close)>(count);
        for (var i = 0; i < count; i++)
        {
            result.Add((ordered[i].TimestampUtc, flattenedCloses[i]));
        }

        return result;
    }

    private sealed record SymbolConfiguration(
        IReadOnlyList<WakettSecuritySymbol> BaseSymbols,
        IReadOnlyList<WakettSecuritySymbol> MissingSymbols,
        int[] AllSecurityIds);

    private sealed record SecuritySymbolDefinition(int SecurityId, CurrencyPair Pair);

    private sealed class SecuritySymbolRow
    {
        public int SecurityId { get; set; }
        public string? Symbol { get; set; }
    }

    internal readonly record struct CurrencyPair(string Base, string Quote);

    internal sealed record ComputedRate(
        WakettSecuritySymbol Definition,
        CurrencyPair Pair,
        decimal Rate);

    internal sealed record DbPriceRecord(
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
