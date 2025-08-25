using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;

namespace TradingDaemon.Services;

public class ReportRunner
{
    private readonly IConfiguration _config;
    private readonly ILogger<ReportRunner> _logger;

    public ReportRunner(IConfiguration config, ILogger<ReportRunner> logger)
    {
        _config = config;
        _logger = logger;
    }

    public async Task<(string StdOut, string StdErr, int ExitCode)> RunAsync(Models.ReportRequest request)
    {
        var pythonExec = _config["Executables:PythonExecutable"] ?? "python3";
        var scriptPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../../scripts/report_model_dwm.py"));
        var args = new StringBuilder();
        args.Append(scriptPath);
        args.Append($" --model-id {request.ModelId} --timeframe {request.Timeframe}");
        if (!string.IsNullOrWhiteSpace(request.FromDate))
            args.Append($" --from-date {request.FromDate}");
        if (!string.IsNullOrWhiteSpace(request.ToDate))
            args.Append($" --to-date {request.ToDate}");
        if (request.AnnualizeDays.HasValue)
            args.Append($" --annualize-days {request.AnnualizeDays.Value}");
        if (request.TopNPairs.HasValue)
            args.Append($" --top-n-pairs {request.TopNPairs.Value}");
        if (!string.IsNullOrWhiteSpace(request.OutputDir))
            args.Append($" --output-dir {request.OutputDir}");

        var sbOut = new StringBuilder();
        var sbErr = new StringBuilder();
        var result = await ProcessRunner.RunAsync(
            pythonExec,
            args.ToString(),
            line =>
            {
                _logger.LogInformation("[report] {Line}", line);
                sbOut.AppendLine(line);
            },
            line =>
            {
                _logger.LogWarning("[report] {Line}", line);
                sbErr.AppendLine(line);
            });

        if (result.ExitCode != 0)
        {
            _logger.LogError("Report script failed: {Error}", sbErr.ToString());
        }
        else
        {
            _logger.LogInformation("Report script completed: {Output}", sbOut.ToString());
        }

        return result;
    }
}
