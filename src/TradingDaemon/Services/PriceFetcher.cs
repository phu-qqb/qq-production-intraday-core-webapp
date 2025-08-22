using System.Globalization;
using System.Runtime.InteropServices;
using System.Linq;
using System.IO;
using System.Data;
using Microsoft.Data.SqlClient;
using Dapper;
using TradingDaemon.Data;
using TradingDaemon.Models;

namespace TradingDaemon.Services;

public class PriceFetcher
{
    private readonly DapperContext _context;
    private readonly ILogger<PriceFetcher> _logger;
    private readonly IConfiguration _config;

    public PriceFetcher(DapperContext context, ILogger<PriceFetcher> logger, IConfiguration config)
    {
        _context = context;
        _logger = logger;
        _config = config;
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
        await connection.ExecuteAsync("DELETE FROM [Intraday].[mkt].[Stage_HistClose]");
        const string insertSql = "INSERT INTO [Intraday].[mkt].[Stage_HistClose] (SecurityId, BarTimeUtc, [Close]) VALUES (@SecurityId, @BarTimeUtc, @Close)";
        await connection.ExecuteAsync(insertSql, records);

        // Load newly staged raw bars into the PriceBar table so that subsequent
        // queries include the latest data.
        await connection.ExecuteAsync("EXEC mkt.LoadRawFromStage @TimeframeMinute = 60");

        // Retrieve all existing raw bars for the affected securities so that
        // flat bars can be recomputed over the full history instead of only
        // the newly provided data.
        const string selectRaw = "SELECT SecurityId, BarTimeUtc, [Close] FROM [Intraday].[mkt].[PriceBar] WHERE TimeframeMinute = 60 AND SecurityId IN @SecurityIds";
        var existing = await connection.QueryAsync<HistClose>(selectRaw, new { SecurityIds = securityIds });

        // Combine existing database bars with the latest file data, removing duplicates
        // by timestamp so that the most recent value for a given bar is used.
        var allBars = existing.Concat(records)
            .GroupBy(r => (r.SecurityId, r.BarTimeUtc))
            .Select(g => g.Last())
            .ToList();

        await connection.ExecuteAsync("DELETE FROM [Intraday].[dbo].[mkt_FlatBar_Staging]");
        var flatRecords = new List<FlatPrice>();
        foreach (var grp in allBars.GroupBy(r => r.SecurityId))
        {
            var ordered = grp.OrderBy(r => r.BarTimeUtc).ToList();
            var rawEU = RawNMin(ordered, 60, "EU", 0);
            var flatEU = Flatten(rawEU, SessionBounds["EU"].Zone)
                .Select(r => new FlatPrice { SecurityId = grp.Key, BarTimeUtc = r.TimestampUtc, Close = r.Close, Session = "EU" });
            flatRecords.AddRange(flatEU);

            var rawUS = RawNMin(ordered, 60, "US", 0);
            var flatUS = Flatten(rawUS, SessionBounds["US"].Zone)
                .Select(r => new FlatPrice { SecurityId = grp.Key, BarTimeUtc = r.TimestampUtc, Close = r.Close, Session = "US" });
            flatRecords.AddRange(flatUS);
        }

        if (flatRecords.Count > 0)
        {
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
                bulkCopy.DestinationTableName = "[Intraday].[dbo].[mkt_FlatBar_Staging]";
                await bulkCopy.WriteToServerAsync(table);
            }

            // Move staged flat bars into the main table for each session.
            foreach (var session in flatRecords.Select(r => r.Session).Distinct())
            {
                await connection.ExecuteAsync(
                    $"EXEC mkt.LoadFlatFromMinimal @TimeframeMinute = 60, @SessionCode = N'{session}'");
            }
        }
    }

    private static readonly Dictionary<string, (TimeZoneInfo Zone, TimeSpan Start, TimeSpan End)> SessionBounds = new()
    {
        ["US"] = (NewYorkZone, TimeSpan.Parse("09:30"), TimeSpan.Parse("15:59")),
        ["EU"] = (NewYorkZone, TimeSpan.Parse("02:00"), TimeSpan.Parse("08:59"))
    };

    private static TimeZoneInfo NewYorkZone => TimeZoneInfo.FindSystemTimeZoneById(
        RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Eastern Standard Time" : "America/New_York");

    private static TimeZoneInfo CentralEuropeZone => TimeZoneInfo.FindSystemTimeZoneById(
        RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Central European Standard Time" : "Europe/Berlin");

    private static List<(DateTime TimestampUtc, decimal Close)> RawNMin(List<HistClose> series, int minutes, string session, int offset)
    {
        var bounds = SessionBounds[session];
        var zone = bounds.Zone;
        var result = new List<(DateTime, decimal)>();
        DateTime? currentBucket = null;
        decimal lastClose = 0;
        foreach (var item in series.OrderBy(s => s.BarTimeUtc))
        {
            var local = TimeZoneInfo.ConvertTimeFromUtc(item.BarTimeUtc, zone);
            if (offset != 0) local = local.AddMinutes(-offset);
            var start = local.TimeOfDay;
            var end = start.Add(TimeSpan.FromMinutes(minutes - 1));
            if (start > bounds.End || end < bounds.Start) continue;
            var bucket = new DateTime(local.Year, local.Month, local.Day, local.Hour, local.Minute / minutes * minutes, 0);
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
}
