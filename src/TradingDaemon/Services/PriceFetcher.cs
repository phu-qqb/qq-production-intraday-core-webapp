using System;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Linq;
using System.IO;
using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using Dapper;
using TradingDaemon.Data;
using TradingDaemon.Models;
using TradingDaemon.Options;

namespace TradingDaemon.Services;

public class PriceFetcher
{
    private readonly DapperContext _context;
    private readonly ILogger<PriceFetcher> _logger;
    private readonly IConfiguration _config;
    private readonly string _stageHistCloseTable;
    private readonly string _flatBarStagingTable;
    private readonly string _stageInsertSql;
    private readonly string _selectRawSql;
    private readonly string _priceBarTable;
    private readonly IPriceProcessingProcedureExecutor _priceProcedures;
    private readonly PriceBarOptions _priceBarOptions;

    public PriceFetcher(
        DapperContext context,
        ILogger<PriceFetcher> logger,
        IConfiguration config,
        IDatabaseObjectNameProvider databaseNameProvider,
        IPriceProcessingProcedureExecutor priceProcedures,
        IOptions<PriceBarOptions>? priceBarOptions = null)
    {
        _context = context;
        _logger = logger;
        _config = config;
        _priceProcedures = priceProcedures;
        _priceBarOptions = priceBarOptions?.Value ?? new PriceBarOptions();
        _stageHistCloseTable = databaseNameProvider.GetObjectName(DatabaseObjects.IntradayMarketStageHistClose);
        _flatBarStagingTable = databaseNameProvider.GetObjectName(DatabaseObjects.IntradayStagingFlatBar);
        _priceBarTable = databaseNameProvider.GetObjectName(DatabaseObjects.IntradayMarketPriceBar);
        _stageInsertSql = $"INSERT INTO {_stageHistCloseTable} (SecurityId, BarTimeUtc, [Close]) VALUES (@SecurityId, @BarTimeUtc, @Close)";
        _selectRawSql = $"SELECT SecurityId, BarTimeUtc, [Close] FROM {_priceBarTable} WHERE TimeframeMinute = {PriceTimeframeMinute} AND SecurityId IN @SecurityIds";
    }

    public async Task FetchAndStoreAsync()
    {
        var filePath = Environment.GetEnvironmentVariable("PRICE_CSV_PATH") ?? _config["PriceCsvPath"];
        if (string.IsNullOrWhiteSpace(filePath) || !File.Exists(filePath))
        {
            _logger.LogWarning("CSV file not found: {FilePath}", filePath);
            return;
        }

        var lines = await File.ReadAllLinesAsync(filePath);
        if (lines.Length < 2) return;

        var headers = lines[0].Split(',');
        var securityIds = headers.Skip(1).ToArray();
        var records = new List<HistClose>();

        for (int i = 1; i < lines.Length; i++)
        {
            var parts = lines[i].Split(',');
            if (parts.Length != headers.Length) continue;
            if (!DateTime.TryParse(parts[0], CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var ts))
                continue;
            for (int j = 1; j < parts.Length; j++)
            {
                if (decimal.TryParse(parts[j], NumberStyles.Any, CultureInfo.InvariantCulture, out var close))
                {
                    records.Add(new HistClose { SecurityId = securityIds[j - 1], BarTimeUtc = ts, Close = close });
                }
            }
        }

        if (records.Count == 0) return;

        using var connection = (SqlConnection)_context.CreateConnection();
        await connection.OpenAsync();
        await connection.ExecuteAsync($"DELETE FROM {_stageHistCloseTable}");
        await connection.ExecuteAsync(_stageInsertSql, records);

        // Load newly staged raw bars into the PriceBar table so that subsequent
        // queries include the latest data.
        await _priceProcedures.LoadRawFromStageAsync(
            connection,
            PriceTimeframeMinute,
            _priceBarOptions.SourceId);

        // Retrieve all existing raw bars for the affected securities so that
        // flat bars can be recomputed over the full history instead of only
        // the newly provided data.
        var existing = await connection.QueryAsync<HistClose>(_selectRawSql, new { SecurityIds = securityIds });

        // Combine existing database bars with the latest file data, removing duplicates
        // by timestamp so that the most recent value for a given bar is used.
        var allBars = existing.Concat(records)
            .GroupBy(r => (r.SecurityId, r.BarTimeUtc))
            .Select(g => g.Last())
            .ToList();

        var seriesBySecurity = allBars
            .GroupBy(r => r.SecurityId)
            .Select(g => new { SecurityId = g.Key, Series = g.OrderBy(r => r.BarTimeUtc).ToList() })
            .ToList();

        var flatBarBuilds = FlatBarBuildSpecificationFactory.CreateDefault(PriceTimeframeMinute, 0);

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

            foreach (var r in flatRecords)
            {
                table.Rows.Add(r.SecurityId, r.BarTimeUtc, r.Close, r.Session);
            }

            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = _flatBarStagingTable;
                await bulkCopy.WriteToServerAsync(table);
            }

            // Move staged flat bars into the main table for each session.
            await _priceProcedures.LoadFlatFromMinimalAsync(connection, build.TimeframeMinute);
        }
    }

    private static readonly Dictionary<string, (TimeZoneInfo Zone, TimeSpan Start, TimeSpan End)> SessionBounds = new()
    {
        ["US"] = (NewYorkZone, TimeSpan.Parse("09:00"), TimeSpan.Parse("15:59")),
        ["EU"] = (NewYorkZone, TimeSpan.Parse("02:00"), TimeSpan.Parse("08:59")),
        ["EUUS"] = (NewYorkZone, TimeSpan.Parse("02:00"), TimeSpan.Parse("11:59"))
    };

    private static TimeZoneInfo NewYorkZone => TimeZoneInfo.FindSystemTimeZoneById(
        RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Eastern Standard Time" : "America/New_York");

    private int PriceTimeframeMinute => Math.Max(1, _priceBarOptions.TimeframeMinute);

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
}
