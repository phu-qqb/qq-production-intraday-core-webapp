using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Dapper;
using TradingDaemon.Data;

namespace TradingDaemon.Services;

public class WeightCalculator
{
    private readonly DapperContext _context;
    private readonly IConfiguration _config;
    private readonly ILogger<WeightCalculator> _logger;

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

        foreach (var model in _config.GetSection("Programmes").GetChildren())
        {
            var universe = model["Universe"] ?? string.Empty;
            var universeId = model["UniverseId"] ?? string.Empty;
            var tradingSession = model["Session"] ?? string.Empty;
            var timeFrame = model["Timeframe"] ?? "60";
            var startDate = model["StartDate"] ?? "2022-01-01";

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

            var exportDir = Path.Combine("/home/data/historical_data", $"Univ{universeId}");
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
                (_config["Executables:GenBinariesExecutable"] ?? string.Empty, $"{universe} {universeId}"),
                (_config["Executables:GenTimeSeriesExecutable"] ?? string.Empty, $"{universe}"),
                (_config["Executables:ProdManagerExecutable"] ?? string.Empty, $"{universe} account={universe}")
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

            var weightsFile = Path.Combine(@"C:\home\prod", universe, "AggregatedWeights.txt");
            if (File.Exists(weightsFile))
            {
                var lines = await File.ReadAllLinesAsync(weightsFile);
                using var connection = _context.CreateConnection();

                connection.Open();

                var modelId = _config.GetValue<int>("ModelId");
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

                    foreach (var line in lines.Skip(1))
                    {
                        _logger.LogInformation("[aggregated-weights] {Line}", line);
                        var parts = line.Split(delimiter, StringSplitOptions.TrimEntries);
                        if (parts.Length <= 1 ||
                            !DateTime.TryParse(parts[0], CultureInfo.InvariantCulture,
                                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var barTimeUtc))
                            continue;

                        for (var i = 1; i < parts.Length && i - 1 < securityIds.Length; i++)
                        {
                            var securityId = securityIds[i - 1];
                            if (securityId is null ||
                                !decimal.TryParse(parts[i], NumberStyles.Any, CultureInfo.InvariantCulture, out var val))
                                continue;

                            var record = new
                            {
                                SecurityId = securityId.Value,
                                ModelId = modelId,
                                BarTimeUtc = barTimeUtc,
                                ModelRunId = modelRunId,
                                Weight = val
                            };

                            await connection.ExecuteAsync(sql, record);
                        }
                    }
                }
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
    }
}
