using Microsoft.Extensions.Logging;
using TradingDaemon.Models;

namespace TradingDaemon.Services;

public sealed record PnlWorkflowResult(PnlReport Report, SlippageResult? SlippageResult);

public sealed class PnlWorkflowRunner
{
    private static TimeZoneInfo NewYorkTimeZone
        => TimeZoneInfo.FindSystemTimeZoneById(
            OperatingSystem.IsWindows() ? "Eastern Standard Time" : "America/New_York");

    private readonly PnlReportService _pnlReportService;
    private readonly SlippageAndMissedCostService _slippageAndMissedCostService;
    private readonly IEmailNotificationService _emailNotificationService;
    private readonly ILogger<PnlWorkflowRunner> _logger;
    private readonly TimeProvider _timeProvider;

    public PnlWorkflowRunner(
        PnlReportService pnlReportService,
        SlippageAndMissedCostService slippageAndMissedCostService,
        IEmailNotificationService emailNotificationService,
        ILogger<PnlWorkflowRunner> logger,
        TimeProvider? timeProvider = null)
    {
        _pnlReportService = pnlReportService;
        _slippageAndMissedCostService = slippageAndMissedCostService;
        _emailNotificationService = emailNotificationService;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<PnlWorkflowResult?> RunAsync(DateOnly? tradingDate, CancellationToken cancellationToken)
    {
        var workflowDate = tradingDate ?? GetCurrentTradingDate();

        try
        {
            var slippageResult = await RunSlippageComputationAsync(workflowDate, cancellationToken);
            var workflowClock = GetWorkflowClockUtc(workflowDate);
            var report = await _pnlReportService.ComputeAndStoreCurrentDayPnlAsync(
                clock: workflowClock,
                cancellationToken: cancellationToken);
            await _emailNotificationService.SendPnLReportAsync(report, slippageResult, cancellationToken);
            return new PnlWorkflowResult(report, slippageResult);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to compute or send automated Wakett PnL report.");
            return null;
        }
    }

    private DateOnly GetCurrentTradingDate()
    {
        var localNow = TimeZoneInfo.ConvertTimeFromUtc(_timeProvider.GetUtcNow().UtcDateTime, NewYorkTimeZone);
        return DateOnly.FromDateTime(localNow);
    }

    private static DateTime GetWorkflowClockUtc(DateOnly tradingDate)
    {
        var localDateTime = tradingDate.ToDateTime(TimeOnly.MinValue);
        return TimeZoneInfo.ConvertTimeToUtc(localDateTime, NewYorkTimeZone);
    }

    private async Task<SlippageResult?> RunSlippageComputationAsync(DateOnly tradingDate, CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Computing slippage and missed trade costs via automation for {TradingDate}.",
            tradingDate);

        try
        {
            return await _slippageAndMissedCostService.ComputeAsync(
                new SlippageRequest { Date = tradingDate.ToDateTime(TimeOnly.MinValue) },
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to compute slippage or missed trade costs for Wakett automation.");
            return null;
        }
    }
}
