using System.IO;
using System.Text;
using Dapper;
using TradingDaemon.Data;
using TradingDaemon.Models;

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
        var pythonExec = _config["PriceExport:PythonExecutable"] ?? "python3";
        var universe = _config["PriceExport:Universe"] ?? string.Empty;
        var tradingSession = _config["PriceExport:Session"] ?? string.Empty;
        var scriptPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../../scripts/export_prices_rds.py"));
        var scriptArgs = string.IsNullOrEmpty(universe) ? scriptPath : $"{scriptPath} --universe {universe} --session {tradingSession}";

        var (pyOut, pyErr, pyCode) = await ProcessRunner.RunAsync(pythonExec, scriptArgs);
        if (pyCode != 0)
        {
            _logger.LogError("Price export script failed: {Error}", pyErr);
            return;
        }

        using var connection = _context.CreateConnection();
        var prices = await connection.QueryAsync<Price>("SELECT symbol, value FROM prices ORDER BY timestamp DESC");

        var inputPath = Path.GetTempFileName();
        await File.WriteAllLinesAsync(inputPath, prices.Select(p => $"{p.Symbol},{p.Value}"));

        var execPath = _config["GpuExecutable"] ?? string.Empty;
        var (stdout, stderr, code) = await ProcessRunner.RunAsync(execPath, inputPath);
        if (code != 0)
        {
            _logger.LogError("GPU process failed: {Error}", stderr);
            return;
        }

        using var reader = new StringReader(stdout);
        string? line;
        while ((line = await reader.ReadLineAsync()) != null)
        {
            var parts = line.Split(',');
            if (parts.Length != 2) continue;
            var weight = new Weight
            {
                Symbol = parts[0],
                Value = decimal.Parse(parts[1]),
                AsOf = DateTime.UtcNow
            };
            var sql = @"INSERT INTO weights (symbol, value, asof) VALUES (@Symbol, @Value, @AsOf)
                        ON CONFLICT (symbol) DO UPDATE SET value = excluded.value, asof = excluded.asof;";
            await connection.ExecuteAsync(sql, weight);
        }

        File.Delete(inputPath);
    }
}
