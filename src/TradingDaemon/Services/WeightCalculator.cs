using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Runtime.InteropServices;
using System.Data;
using Dapper;
using TradingDaemon.Data;

namespace TradingDaemon.Services;

public class WeightCalculator
{
    private readonly DapperContext _context;
    private readonly IConfiguration _config;
    private readonly ILogger<WeightCalculator> _logger;

    private static readonly IReadOnlyDictionary<string, SessionInfo> SessionBounds =
        new Dictionary<string, SessionInfo>(StringComparer.OrdinalIgnoreCase)
        {
            ["US"] = new SessionInfo(ResolveTimeZone("Eastern Standard Time", "America/New_York"),
                TimeSpan.Parse("09:00"), TimeSpan.Parse("15:59")),
            ["EU"] = new SessionInfo(ResolveTimeZone("Eastern Standard Time", "America/New_York"),
                TimeSpan.Parse("02:00"), TimeSpan.Parse("08:59"))
        };

    public WeightCalculator(DapperContext context, IConfiguration config, ILogger<WeightCalculator> logger)
    {
        _context = context;
        _config = config;
        _logger = logger;
    }

    public async Task CalculateAndStoreAsync()
    {
        var pythonExec = _config["Executables:PythonExecutable"] ?? "python3";
        var scriptPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../../scripts/export_prices_rds.py"));

        var modelTimeframes = new Dictionary<int, int>();

        var modelSessions = new Dictionary<int, string>();

        var priceOffset = _config.GetValue<int?>("ExternalApis:WakettApi:PriceMinuteOffset") ?? 0;

        foreach (var model in _config.GetSection("Programmes").GetChildren())
        {
            var universe = model["Universe"] ?? string.Empty;
            var universeId = model["UniverseId"] ?? string.Empty;
            var tradingSession = model["Session"] ?? string.Empty;
            var timeFrame = model["Timeframe"] ?? "60";
            var startDate = model["StartDate"] ?? "2022-01-01";
            var modelId = int.Parse(model["ModelId"] ?? "0");
            var timeFrameInt = int.TryParse(timeFrame, out var tfVal) ? tfVal : 60;

            modelTimeframes[modelId] = timeFrameInt;
            if (!string.IsNullOrWhiteSpace(tradingSession))
            {
                modelSessions[modelId] = tradingSession.Trim();
            }

            var scriptArgs = string.IsNullOrEmpty(universe)
                ? scriptPath
                : $"{scriptPath} --universe {universe} --session {tradingSession} --timeframe {timeFrame} --start {startDate}";

            var sbOut = new StringBuilder();
            var sbErr = new StringBuilder();
            var (_, _, pyCode) = await ProcessRunner.RunAsync(
                pythonExec,
                scriptArgs,
                line =>
                {
                    _logger.LogInformation("[price-export] {Line}", line);
                    sbOut.AppendLine(line);
                },
                line =>
                {
                    _logger.LogWarning("[price-export] {Line}", line);
                    sbErr.AppendLine(line);
                });
            if (pyCode != 0)
            {
                _logger.LogError("Price export script failed for {Universe}: {Error}", universe, sbErr.ToString());
                continue;
            }
            _logger.LogInformation("Price export script completed successfully for {Universe}: {Output}", universe, sbOut.ToString());

            var exportDir = ResolveHomePath(Path.Combine("/home/data/historical_data", $"Univ{universeId}"));
            foreach (var name in new[] { "A", "H", "I" })
            {
                var path = Path.Combine(exportDir, $"{name}.txt");
                if (File.Exists(path))
                {
                    var size = new FileInfo(path).Length;
                    _logger.LogInformation("Found export file {File} ({Size} bytes)", path, size);
                }
                else
                {
                    _logger.LogWarning("Missing export file {File}", path);
                }
            }

            var executables = new List<(string Path, string Args)>
            {
                (ResolveHomePath(_config["Executables:GenBinariesExecutable"] ?? string.Empty), $"{universe} {universeId}"),
                (ResolveHomePath(_config["Executables:GenTimeSeriesExecutable"] ?? string.Empty), $"{universe}"),
                (ResolveHomePath(_config["Executables:ProdManagerExecutable"] ?? string.Empty), $"{universe} account={universe}")
            };

            string stdout = string.Empty;
            foreach (var (path, args) in executables)
            {
                if (string.IsNullOrWhiteSpace(path)) continue;
                var commandLine = $"{path} {args}".Trim();
                _logger.LogInformation("Executing command: {Command}", commandLine);
                var (outText, errText, exit) = await ProcessRunner.RunAsync(path, args);
                if (exit != 0)
                {
                    var message = $"Executable failed: {commandLine} (exit code {exit})";
                    _logger.LogError("{Message}. Error output: {Error}", message, errText);
                    if (OperatingSystem.IsWindows())
                    {
                        try
                        {
                            var type = Type.GetType("System.Windows.Forms.MessageBox, System.Windows.Forms");
                            type?.GetMethod("Show", new[] { typeof(string), typeof(string) })?
                                .Invoke(null, new object[] { $"{message}\n{errText}", "Execution Error" });
                        }
                        catch
                        {
                            // ignore any reflection errors
                        }
                    }
                    return;
                }
                _logger.LogInformation("Executable {Exec} completed: {Output}", path, outText);
                stdout = outText;
            }

            var weightsFile = ResolveHomePath(Path.Combine(@"C:\home\prod", universe, "AggregatedWeights.txt"));
            if (File.Exists(weightsFile))
            {
                var lines = await File.ReadAllLinesAsync(weightsFile);
                using var connection = _context.CreateConnection();

                connection.Open();

                var version = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version?.ToString();
                var modelRunId = await connection.ExecuteScalarAsync<long>(
                    "INSERT INTO model.ModelRun (ModelId, CodeVersion) VALUES (@ModelId, @CodeVersion); SELECT CAST(SCOPE_IDENTITY() AS bigint);",
                    new { ModelId = modelId, CodeVersion = version });

                if (lines.Length > 1)
                {
                    var delimiter = lines[0].Contains(';') ? ';' : ',';
                    var headerParts = lines[0].Split(delimiter, StringSplitOptions.TrimEntries);
                    var securityIds = headerParts.Skip(1)
                        .Select(h => long.TryParse(h, out var id) ? id : (long?)null)
                        .ToArray();

                    var sql = @"IF NOT EXISTS (
    SELECT 1 FROM model.TheoreticalWeight
    WHERE SecurityId = @SecurityId AND ModelId = @ModelId AND BarTimeUtc = @BarTimeUtc
)
BEGIN
    INSERT INTO model.TheoreticalWeight (SecurityId, ModelId, BarTimeUtc, ModelRunId, Weight)
    VALUES (@SecurityId, @ModelId, @BarTimeUtc, @ModelRunId, @Weight);
END";

                    var rows = new List<WeightRow>();

                    foreach (var line in lines.Skip(1))
                    {
                        var parts = line.Split(delimiter, StringSplitOptions.TrimEntries);
                        if (parts.Length <= 1 ||
                            !DateTime.TryParseExact(
                                parts[0],
                                "yyyyMMddHHmm",
                                CultureInfo.InvariantCulture,
                                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                                out var barTimeUtc))
                                    continue;

                        var weightArr = new decimal?[securityIds.Length];
                        for (var i = 1; i < parts.Length && i - 1 < securityIds.Length; i++)
                        {
                            if (decimal.TryParse(parts[i], NumberStyles.Any, CultureInfo.InvariantCulture, out var val))
                            {
                                weightArr[i - 1] = val;
                            }
                        }

                        rows.Add(new WeightRow(barTimeUtc, weightArr));
                    }

                    ZeroPenultimateRows(rows, modelSessions.GetValueOrDefault(modelId), timeFrameInt, priceOffset);

                    foreach (var row in rows)
                    {
                        var inserted = false;
                        for (var i = 0; i < row.Weights.Length && i < securityIds.Length; i++)
                        {
                            var securityId = securityIds[i];
                            var val = row.Weights[i];
                            if (securityId is null || val is null)
                                continue;

                            var record = new
                            {
                                SecurityId = securityId.Value,
                                ModelId = modelId,
                                BarTimeUtc = row.BarTimeUtc,
                                ModelRunId = modelRunId,
                                Weight = val.Value
                            };

                            var affected = await connection.ExecuteAsync(sql, record);
                            if (affected > 0) inserted = true;
                        }

                        if (inserted)
                        {
                            var lineOut = string.Join(delimiter,
                                new[] { row.BarTimeUtc.ToString("yyyyMMddHHmm") }
                                    .Concat(row.Weights.Select(w => w?.ToString(CultureInfo.InvariantCulture) ?? string.Empty)));
                            _logger.LogInformation("[aggregated-weights] {Line}", lineOut);
                        }
                    }
                }

                await ComputeNettedWeights(connection, modelId, modelRunId);
            }
            else
            {
                _logger.LogWarning("Missing weights file {File}", weightsFile);
            }

            //using var connection = _context.CreateConnection();
            //var prices = await connection.QueryAsync<Price>("SELECT symbol, value FROM prices ORDER BY timestamp DESC");

            //var inputPath = Path.GetTempFileName();
            //await File.WriteAllLinesAsync(inputPath, prices.Select(p => $"{p.Symbol},{p.Value}"));

            //if (!string.IsNullOrWhiteSpace(execPath))
            //{
            //    var (stdout, stderr, code) = await ProcessRunner.RunAsync(execPath, inputPath);
            //    if (code != 0)
            //    {
            //        _logger.LogError("Executable {Exec} failed: {Error}", execPath, stderr);
            //        File.Delete(inputPath);
            //        continue;
            //    }

            //    using var reader = new StringReader(stdout);
            //    string? line;
            //    while ((line = await reader.ReadLineAsync()) != null)
            //    {
            //        var parts = line.Split(',');
            //        if (parts.Length != 2) continue;
            //        var weight = new Weight
            //        {
            //            Symbol = parts[0],
            //            Value = decimal.Parse(parts[1]),
            //            AsOf = DateTime.UtcNow
            //        };
            //        var sql = @"INSERT INTO weights (symbol, value, asof) VALUES (@Symbol, @Value, @AsOf)
            //                    ON CONFLICT (symbol) DO UPDATE SET value = excluded.value, asof = excluded.asof;";
            //        await connection.ExecuteAsync(sql, weight);
            //    }
            //}

            //File.Delete(inputPath);
        }

        await RunModelReportsAsync(modelTimeframes);
    }

    private void ZeroPenultimateRows(List<WeightRow> rows, string? sessionKey, int timeframeMinutes, int offsetMinutes)
    {
        if (rows.Count == 0)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(sessionKey) || !SessionBounds.TryGetValue(sessionKey.Trim(), out var session))
        {
            ZeroPenultimateByCalendarDay(rows);
            return;
        }

        var alignedStart = AlignSessionStart(session.Start, timeframeMinutes);
        var groups = rows
            .GroupBy(r => GetSessionStartLocal(r.BarTimeUtc, session, alignedStart, offsetMinutes));

        foreach (var group in groups)
        {
            var ordered = group.OrderBy(r => r.BarTimeUtc).ToList();
            var expected = GetExpectedPenultimateUtc(group.Key, session, alignedStart, timeframeMinutes, offsetMinutes);
            if (!expected.HasValue)
            {
                continue;
            }

            var target = ordered.FirstOrDefault(r => r.BarTimeUtc == expected.Value);
            if (target is null)
            {
                _logger.LogWarning(
                    "Unable to locate expected penultimate bar {ExpectedUtc} for session {Session} (start {SessionStart})",
                    expected.Value,
                    sessionKey,
                    group.Key);
                continue;
            }

            ZeroRow(target);
        }
    }

    private static void ZeroPenultimateByCalendarDay(List<WeightRow> rows)
    {
        foreach (var group in rows.GroupBy(r => r.BarTimeUtc.Date))
        {
            var ordered = group.OrderBy(r => r.BarTimeUtc).ToList();
            if (ordered.Count < 2)
            {
                continue;
            }

            ZeroRow(ordered[^2]);
        }
    }

    private static void ZeroRow(WeightRow? row)
    {
        if (row is null)
        {
            return;
        }

        for (var i = 0; i < row.Weights.Length; i++)
        {
            if (row.Weights[i].HasValue)
            {
                row.Weights[i] = 0m;
            }
        }
    }

    private static DateTime GetSessionStartLocal(DateTime barTimeUtc, SessionInfo session, TimeSpan alignedStart, int offsetMinutes)
    {
        var local = TimeZoneInfo.ConvertTimeFromUtc(barTimeUtc, session.Zone);
        if (offsetMinutes != 0)
        {
            local = local.AddMinutes(-offsetMinutes);
        }

        var startLocal = local.Date.Add(alignedStart);
        if (SessionWrapsMidnight(session.Start, session.End) || local.TimeOfDay < alignedStart)
        {
            if (local.TimeOfDay < alignedStart)
            {
                startLocal = startLocal.AddDays(-1);
            }
        }

        return startLocal;
    }

    private static DateTime? GetExpectedPenultimateUtc(
        DateTime sessionStartLocal,
        SessionInfo session,
        TimeSpan alignedStart,
        int timeframeMinutes,
        int offsetMinutes)
    {
        var bucketCount = GetSessionBucketCount(alignedStart, session.End, timeframeMinutes, SessionWrapsMidnight(session.Start, session.End));
        if (bucketCount < 2)
        {
            return null;
        }

        var penultimateStartLocal = sessionStartLocal.AddMinutes(timeframeMinutes * (bucketCount - 2));
        var penultimateLocalWithOffset = penultimateStartLocal.AddMinutes(offsetMinutes);
        return TimeZoneInfo.ConvertTimeToUtc(penultimateLocalWithOffset, session.Zone);
    }

    private static int GetSessionBucketCount(TimeSpan sessionStartAligned, TimeSpan sessionEnd, int timeframeMinutes, bool wrapsMidnight)
    {
        if (timeframeMinutes <= 0)
        {
            return 0;
        }

        int totalMinutes;
        if (!wrapsMidnight)
        {
            if (sessionEnd < sessionStartAligned)
            {
                return 0;
            }

            totalMinutes = (int)(sessionEnd - sessionStartAligned).TotalMinutes + 1;
        }
        else
        {
            totalMinutes = (int)((TimeSpan.FromHours(24) - sessionStartAligned + sessionEnd).TotalMinutes) + 1;
        }

        if (totalMinutes < timeframeMinutes)
        {
            return 0;
        }

        return totalMinutes / timeframeMinutes;
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

    private static bool SessionWrapsMidnight(TimeSpan sessionStart, TimeSpan sessionEnd)
    {
        return sessionEnd < sessionStart;
    }

    private static TimeZoneInfo ResolveTimeZone(string windowsId, string linuxId)
    {
        return TimeZoneInfo.FindSystemTimeZoneById(RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? windowsId : linuxId);
    }

    private sealed record SessionInfo(TimeZoneInfo Zone, TimeSpan Start, TimeSpan End);

    private async Task RunModelReportsAsync(Dictionary<int, int> modelTimeframes)
    {
        using var connection = _context.CreateConnection();

        connection.Open();

        var toUtc = DateTime.UtcNow.Date;
        var fromUtc = toUtc.AddDays(-10);
        var fromDate = new DateTime(toUtc.Year, 1, 1);
        var toDate = new DateTime(toUtc.Year, toUtc.Month, 1).AddMonths(1);

        foreach (var kvp in modelTimeframes)
        {
            var modelId = kvp.Key;
            var timeframe = kvp.Value;

            await connection.ExecuteAsync(
                "model.ComputeAndStoreModelBarPnL",
                new { ModelId = modelId, TimeframeMinute = timeframe, FromUtc = fromUtc, ToUtc = toUtc, UseLogReturn = 0 },
                commandType: CommandType.StoredProcedure);

            await connection.ExecuteAsync(
                "model.Report_ModelDWM",
                new { ModelId = modelId, TimeframeMinute = timeframe, FromDate = fromDate, ToDate = toDate, AnnualizeDays = 252 },
                commandType: CommandType.StoredProcedure);
        }
    }

    private async Task ComputeNettedWeights(IDbConnection connection, int modelId, long modelRunId)
    {
        var weights = await connection.QueryAsync<(long SecurityId, DateTime BarTimeUtc, decimal Weight, string Ticker)>(
            @"SELECT tw.SecurityId, tw.BarTimeUtc, tw.Weight, s.BloombergTicker
              FROM model.TheoreticalWeight tw
              JOIN core.Security s ON tw.SecurityId = s.SecurityId
              WHERE tw.ModelId = @ModelId AND tw.ModelRunId = @ModelRunId",
            new { ModelId = modelId, ModelRunId = modelRunId });

        var usdPairs = await connection.QueryAsync<(long SecurityId, string Ticker)>(
            @"SELECT SecurityId, BloombergTicker FROM core.Security WHERE BloombergTicker LIKE '%USD%'");

        var usdMap = BuildUsdMap(usdPairs);
        var usdBaseIds = new HashSet<long>(usdPairs
            .Where(p =>
            {
                var pair = p.Ticker.Split(' ')[0];
                return pair.Length >= 6 && pair[..3] == "USD";
            })
            .Select(p => p.SecurityId));

        var net = new Dictionary<(long SecurityId, DateTime BarTimeUtc), decimal>();

        foreach (var w in weights)
        {
            var pair = w.Ticker.Split(' ')[0];
            if (pair.Length < 6) continue;
            var baseCcy = pair[..3];
            var quoteCcy = pair.Substring(3, 3);
            if(pair.Contains("NZD"))
            {
                int u = 0;
            }

            if (baseCcy == "USD" || quoteCcy == "USD")
            {
                var (secId, weight) = NormalizeUsdPair(w.SecurityId, pair, w.Weight, usdMap);
                var key = (secId, w.BarTimeUtc);
                net[key] = net.GetValueOrDefault(key) + weight;
                continue;
            }

            if (usdMap.TryGetValue((baseCcy, "USD"), out var baseId))
            {
                var key = (baseId, w.BarTimeUtc);
                net[key] = net.GetValueOrDefault(key) + w.Weight;
            }
            else if (usdMap.TryGetValue(("USD", baseCcy), out var invBaseId))
            {
                var key = (invBaseId, w.BarTimeUtc);
                net[key] = net.GetValueOrDefault(key) - w.Weight;
            }

            if (usdMap.TryGetValue((quoteCcy, "USD"), out var quoteId))
            {
                var key = (quoteId, w.BarTimeUtc);
                net[key] = net.GetValueOrDefault(key) - w.Weight;
            }
            else if (usdMap.TryGetValue(("USD", quoteCcy), out var invQuoteId))
            {
                var key = (invQuoteId, w.BarTimeUtc);
                net[key] = net.GetValueOrDefault(key) + w.Weight;
            }
        }

        var insertSql = @"IF NOT EXISTS (
    SELECT 1 FROM model.NettedWeight
    WHERE SecurityId = @SecurityId AND ModelId = @ModelId AND BarTimeUtc = @BarTimeUtc
)
BEGIN
    INSERT INTO model.NettedWeight (SecurityId, ModelId, BarTimeUtc, ModelRunId, Weight)
    VALUES (@SecurityId, @ModelId, @BarTimeUtc, @ModelRunId, @Weight);
END";

        foreach (var entry in net)
        {
            var weight = AdjustWeightForUsdBase(entry.Key.SecurityId, entry.Value, usdBaseIds);
            var record = new
            {
                SecurityId = entry.Key.SecurityId,
                ModelId = modelId,
                BarTimeUtc = entry.Key.BarTimeUtc,
                ModelRunId = modelRunId,
                Weight = weight
            };

            await connection.ExecuteAsync(insertSql, record);
        }
    }

    private static Dictionary<(string Base, string Quote), long> BuildUsdMap(IEnumerable<(long SecurityId, string Ticker)> usdPairs)
    {
        var usdMap = new Dictionary<(string Base, string Quote), long>();
        foreach (var p in usdPairs)
        {
            var pair = p.Ticker.Split(' ')[0];
            if (pair.Length < 6) continue;
            var baseCcy = pair[..3];
            var quoteCcy = pair.Substring(3, 3);

            if (quoteCcy == "USD")
            {
                // prefer mappings where USD is the quote currency
                usdMap[(baseCcy, quoteCcy)] = p.SecurityId;
            }
            else if (baseCcy == "USD" && !usdMap.ContainsKey((quoteCcy, "USD")))
            {
                // fall back to inverse pairs if the USD-quote pair is missing
                usdMap[(quoteCcy, "USD")] = p.SecurityId;
            }
        }

        return usdMap;
    }

    private static (long SecurityId, decimal Weight) NormalizeUsdPair(
        long securityId,
        string pair,
        decimal weight,
        Dictionary<(string Base, string Quote), long> usdMap)
    {
        var baseCcy = pair[..3];
        var quoteCcy = pair.Substring(3, 3);

        if (quoteCcy == "USD")
        {
            if (usdMap.TryGetValue((baseCcy, "USD"), out var canonId))
            {
                return (canonId, weight);
            }

            return (securityId, weight);
        }

        if (baseCcy == "USD")
        {
            if (usdMap.TryGetValue((quoteCcy, "USD"), out var canonId))
            {
                return (canonId, -weight);
            }

            return (securityId, -weight);
        }

        return (securityId, -weight);
    }

    private static decimal AdjustWeightForUsdBase(long securityId, decimal weight, HashSet<long> usdBaseIds)
    {
        return usdBaseIds.Contains(securityId) ? -weight : weight;
    }

    private static string ResolveHomePath(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return path ?? string.Empty;
        }

        var resolved = path;

        var unixRoot = Environment.GetEnvironmentVariable("HOME_ROOT");
        if (string.IsNullOrEmpty(unixRoot))
        {
            var environmentName =
                Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ??
                Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT");

            if (string.Equals(environmentName, "Test", StringComparison.OrdinalIgnoreCase))
            {
                unixRoot = "/home_test";
            }
        }
        if (!string.IsNullOrEmpty(unixRoot))
        {
            const string unixPrefix = "/home";
            if (string.Equals(resolved, unixPrefix, StringComparison.Ordinal) ||
                string.Equals(resolved, unixPrefix + "/", StringComparison.Ordinal))
            {
                resolved = unixRoot;
            }
            else if (resolved.StartsWith(unixPrefix + "/", StringComparison.Ordinal))
            {
                resolved = CombinePath(unixRoot, resolved.Substring(unixPrefix.Length + 1));
            }
        }

        var windowsRoot = Environment.GetEnvironmentVariable("WINDOWS_HOME_ROOT");
        if (!string.IsNullOrEmpty(windowsRoot))
        {
            foreach (var prefix in new[] { @"C:\\home\\", @"C:/home/", @"C:\\home", @"C:/home" })
            {
                if (resolved.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                {
                    var relative = resolved.Length > prefix.Length
                        ? resolved.Substring(prefix.Length).TrimStart('/', '\\')
                        : string.Empty;
                    resolved = CombinePath(windowsRoot, relative);
                    break;
                }
            }
        }

        return resolved;
    }

    private static string CombinePath(string root, string? relative)
    {
        if (string.IsNullOrEmpty(relative))
        {
            return root;
        }

        var segments = relative
            .Split(new[] { '/', '\\' }, StringSplitOptions.RemoveEmptyEntries);

        return segments.Aggregate(root, Path.Combine);
    }

    private sealed class WeightRow
    {
        public DateTime BarTimeUtc { get; }
        public decimal?[] Weights { get; set; }

        public WeightRow(DateTime barTimeUtc, decimal?[] weights)
        {
            BarTimeUtc = barTimeUtc;
            Weights = weights;
        }
    }
}
