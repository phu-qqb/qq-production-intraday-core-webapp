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
