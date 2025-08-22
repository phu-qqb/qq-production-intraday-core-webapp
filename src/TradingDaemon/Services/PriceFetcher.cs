using System.Globalization;
using System.Runtime.InteropServices;
using System.Linq;
using System.IO;
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

        using var connection = _context.CreateConnection();
        await connection.ExecuteAsync("DELETE FROM [Intraday].[mkt].[Stage_HistClose]");
        const string insertSql = "INSERT INTO [Intraday].[mkt].[Stage_HistClose] (SecurityId, BarTimeUtc, Close) VALUES (@SecurityId, @BarTimeUtc, @Close)";
        await connection.ExecuteAsync(insertSql, records);

        // Retrieve all existing raw bars for the affected securities so that
        // flat bars can be recomputed over the full history instead of only
        // the newly provided data.
        const string selectRaw = "SELECT SecurityId, BarTimeUtc, Close FROM [Intraday].[mkt].[PriceBar] WHERE TimeframeMinute = 60 AND SecurityId IN @SecurityIds";
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
            var flatEU = Flatten(rawEU)
                .Select(r => new FlatPrice { SecurityId = grp.Key, BarTimeUtc = r.TimestampUtc, Close = r.Close, Session = "EU" });
            flatRecords.AddRange(flatEU);

            var rawUS = RawNMin(ordered, 60, "US", 0);
            var flatUS = Flatten(rawUS)
                .Select(r => new FlatPrice { SecurityId = grp.Key, BarTimeUtc = r.TimestampUtc, Close = r.Close, Session = "US" });
            flatRecords.AddRange(flatUS);
        }

        if (flatRecords.Count > 0)
        {
            const string insertFlat = "INSERT INTO [Intraday].[dbo].[mkt_FlatBar_Staging] (SecurityId, BarTimeUtc, Close, Session) VALUES (@SecurityId, @BarTimeUtc, @Close, @Session)";
            await connection.ExecuteAsync(insertFlat, flatRecords);
        }
    }

    private static readonly Dictionary<string, (TimeSpan Start, TimeSpan End)> SessionBounds = new()
    {
        ["US"] = (TimeSpan.Parse("09:30"), TimeSpan.Parse("15:59")),
        ["EU"] = (TimeSpan.Parse("02:00"), TimeSpan.Parse("08:59"))
    };

    private static TimeZoneInfo NewYorkZone => TimeZoneInfo.FindSystemTimeZoneById(
        RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Eastern Standard Time" : "America/New_York");

    private static List<(DateTime TimestampUtc, decimal Close)> RawNMin(List<HistClose> series, int minutes, string session, int offset)
    {
        var bounds = SessionBounds[session];
        var result = new List<(DateTime, decimal)>();
        DateTime? currentBucket = null;
        decimal lastClose = 0;
        foreach (var item in series.OrderBy(s => s.BarTimeUtc))
        {
            var ny = TimeZoneInfo.ConvertTimeFromUtc(item.BarTimeUtc, NewYorkZone);
            if (offset != 0) ny = ny.AddMinutes(-offset);
            var tod = ny.TimeOfDay;
            if (tod < bounds.Start || tod > bounds.End) continue;
            var bucket = new DateTime(ny.Year, ny.Month, ny.Day, ny.Hour, ny.Minute / minutes * minutes, 0);
            if (currentBucket != bucket)
            {
                if (currentBucket.HasValue)
                    result.Add((TimeZoneInfo.ConvertTimeToUtc(currentBucket.Value.AddMinutes(offset), NewYorkZone), lastClose));
                currentBucket = bucket;
            }
            lastClose = item.Close;
        }
        if (currentBucket.HasValue)
            result.Add((TimeZoneInfo.ConvertTimeToUtc(currentBucket.Value.AddMinutes(offset), NewYorkZone), lastClose));
        return result;
    }

    private static List<(DateTime TimestampUtc, decimal Close)> Flatten(List<(DateTime TimestampUtc, decimal Close)> raw)
    {
        if (raw.Count == 0) return new();
        var times = raw.Select(r => r.TimestampUtc).ToList();
        var px = raw.Select(r => r.Close).ToList();
        var nyTimes = times.Select(t => TimeZoneInfo.ConvertTimeFromUtc(t, NewYorkZone)).ToList();
        var ret = new decimal[px.Count];
        for (int i = 1; i < px.Count; i++)
        {
            var prev = px[i - 1];
            ret[i] = prev != 0 ? (px[i] - prev) / prev : 0m;
            if (nyTimes[i].Date != nyTimes[i - 1].Date)
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
