using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TradingDaemon.Models;

namespace TradingDaemon.Services;

public sealed class WakettAutomationService : BackgroundService
{
    private static readonly TimeSpan SessionStart = TimeSpan.FromHours(9);
    private static readonly TimeSpan SessionEnd = new(15, 59, 0);
    private static readonly TimeSpan SessionShutdownDelay = TimeSpan.FromHours(1);

    private static readonly int[] PriceFetchMinutes = { 20, 50 };
    private static readonly int[] WeightCalculationMinutes = { 25, 55 };
    private static readonly int[] FillCheckMinutes = { 10, 40 };
    private static readonly int[] OrderSubmissionMinutes = { 1 };
    private static readonly int[] PnlReportMinutes = { 15 };

    private readonly WakettPriceFetcher _priceFetcher;
    private readonly WeightCalculator _weightCalculator;
    private readonly OrderSender _orderSender;
    private readonly WakettTradeFetcher _tradeFetcher;
    private readonly PnlReportService _pnlReportService;
    private readonly IEmailNotificationService _emailNotificationService;
    private readonly IHostApplicationLifetime _applicationLifetime;
    private readonly ILogger<WakettAutomationService> _logger;
    private readonly WakettAutomationOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly IConfiguration _configuration;

    public WakettAutomationService(
        WakettPriceFetcher priceFetcher,
        WeightCalculator weightCalculator,
        OrderSender orderSender,
        WakettTradeFetcher tradeFetcher,
        PnlReportService pnlReportService,
        IEmailNotificationService emailNotificationService,
        IHostApplicationLifetime applicationLifetime,
        IOptions<WakettAutomationOptions> options,
        ILogger<WakettAutomationService> logger,
        IConfiguration configuration,
        TimeProvider? timeProvider = null)
    {
        _priceFetcher = priceFetcher;
        _weightCalculator = weightCalculator;
        _orderSender = orderSender;
        _tradeFetcher = tradeFetcher;
        _pnlReportService = pnlReportService;
        _emailNotificationService = emailNotificationService;
        _applicationLifetime = applicationLifetime;
        _logger = logger;
        _options = options?.Value ?? new WakettAutomationOptions();
        _configuration = configuration;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting Wakett automation service.");

        var sessionActive = false;
        var sessionShutdownDeadlineUtc = (DateTime?)null;
        var currentSessionStartUtc = DateTime.MinValue;
        var currentSessionEndUtc = DateTime.MinValue;
        var nextSessionStartUtc = GetNextSessionStartUtc(_timeProvider.GetUtcNow().UtcDateTime);

        var nextPriceFetchUtc = DateTime.MaxValue;
        var nextWeightCalculationUtc = DateTime.MaxValue;
        var nextOrderSubmissionUtc = DateTime.MaxValue;
        var nextFillCheckUtc = DateTime.MaxValue;
        var nextPnlReportUtc = DateTime.MaxValue;

        while (!stoppingToken.IsCancellationRequested)
        {
            var nowUtc = _timeProvider.GetUtcNow().UtcDateTime;

            if (sessionActive)
            {
                if (nowUtc > currentSessionEndUtc)
                {
                    sessionActive = false;
                    sessionShutdownDeadlineUtc = GetSessionShutdownDeadlineUtc(nowUtc);
                    nextSessionStartUtc = GetNextSessionStartUtc(nowUtc);
                    nextPriceFetchUtc = DateTime.MaxValue;
                    nextWeightCalculationUtc = DateTime.MaxValue;
                    nextOrderSubmissionUtc = DateTime.MaxValue;
                    nextFillCheckUtc = DateTime.MaxValue;
                    nextPnlReportUtc = DateTime.MaxValue;
                    _logger.LogInformation(
                        "Exited Wakett session window at {NowUtc:o}. Next session begins at {SessionStartUtc:o}.",
                        nowUtc,
                        nextSessionStartUtc);
                    continue;
                }

                var nextEventUtc = GetEarliest(nextPriceFetchUtc, nextWeightCalculationUtc, nextOrderSubmissionUtc, nextFillCheckUtc, nextPnlReportUtc);

                if (nextEventUtc == DateTime.MaxValue)
                {
                    nextEventUtc = currentSessionEndUtc;
                }

                if (nowUtc < nextEventUtc)
                {
                    await DelayUntilAsync(nextEventUtc, stoppingToken);
                    continue;
                }

                if (nowUtc >= nextPriceFetchUtc)
                {
                    await RunPriceFetchAsync(stoppingToken);
                    nextPriceFetchUtc = GetNextSessionEventUtc(
                        nowUtc.AddSeconds(1),
                        PriceFetchMinutes,
                        currentSessionStartUtc,
                        currentSessionEndUtc);
                }

                if (nowUtc >= nextWeightCalculationUtc)
                {
                    await RunWeightCalculationAsync(stoppingToken);
                    nextWeightCalculationUtc = GetNextSessionEventUtc(
                        nowUtc.AddSeconds(1),
                        WeightCalculationMinutes,
                        currentSessionStartUtc,
                        currentSessionEndUtc);
                }

                if (nowUtc >= nextOrderSubmissionUtc)
                {
                    await RunOrderSubmissionAsync(stoppingToken);
                    nextOrderSubmissionUtc = GetNextSessionEventUtc(
                        nowUtc.AddSeconds(1),
                        OrderSubmissionMinutes,
                        currentSessionStartUtc,
                        currentSessionEndUtc);
                }

                if (nowUtc >= nextFillCheckUtc)
                {
                    await RunFillCheckAsync(stoppingToken);
                    nextFillCheckUtc = GetNextSessionEventUtc(
                        nowUtc.AddSeconds(1),
                        FillCheckMinutes,
                        currentSessionStartUtc,
                        currentSessionEndUtc);
                }

                if (nowUtc >= nextPnlReportUtc)
                {
                    await RunPnlWorkflowAsync(stoppingToken);
                    nextPnlReportUtc = GetNextSessionEventUtc(
                        nowUtc.AddSeconds(1),
                        PnlReportMinutes,
                        currentSessionStartUtc,
                        currentSessionEndUtc);
                }

                continue;
            }

            if (sessionShutdownDeadlineUtc is { } shutdownUtc && nowUtc >= shutdownUtc)
            {
                _logger.LogInformation("Post-session buffer elapsed. Shutting down application.");
                _applicationLifetime.StopApplication();
                return;
            }

            if (nowUtc >= nextSessionStartUtc)
            {
                sessionActive = true;
                sessionShutdownDeadlineUtc = null;
                currentSessionStartUtc = nextSessionStartUtc;
                currentSessionEndUtc = GetSessionEndUtc(currentSessionStartUtc);
                _logger.LogInformation(
                    "Entering Wakett session window at {NowUtc:o}. Session ends at {SessionEndUtc:o}.",
                    nowUtc,
                    currentSessionEndUtc);

                nextPriceFetchUtc = GetNextSessionEventUtc(nowUtc, PriceFetchMinutes, currentSessionStartUtc, currentSessionEndUtc);
                nextWeightCalculationUtc = GetNextSessionEventUtc(nowUtc, WeightCalculationMinutes, currentSessionStartUtc, currentSessionEndUtc);
                nextOrderSubmissionUtc = GetNextSessionEventUtc(nowUtc, OrderSubmissionMinutes, currentSessionStartUtc, currentSessionEndUtc);
                nextFillCheckUtc = GetNextSessionEventUtc(nowUtc, FillCheckMinutes, currentSessionStartUtc, currentSessionEndUtc);
                nextPnlReportUtc = GetNextSessionEventUtc(nowUtc, PnlReportMinutes, currentSessionStartUtc, currentSessionEndUtc);
                continue;
            }

            var nextDelayTargetUtc = nextSessionStartUtc;
            if (sessionShutdownDeadlineUtc is { } shutdownDeadline)
            {
                if (shutdownDeadline < nextDelayTargetUtc)
                {
                    nextDelayTargetUtc = shutdownDeadline;
                }
            }

            await DelayUntilAsync(nextDelayTargetUtc, stoppingToken);
        }
    }

    private async Task RunPriceFetchAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Fetching Wakett prices as part of automated schedule.");

        try
        {
            await _priceFetcher.FetchAndStoreAsync(stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed to fetch Wakett prices.");
        }
    }

    private async Task RunWeightCalculationAsync(CancellationToken stoppingToken)
    {
        bool pricesComplete;
        try
        {
            pricesComplete = await _priceFetcher.AreRecentPricesCompleteAsync(stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed to verify Wakett prices completeness before weight calculation.");
            return;
        }

        if (!pricesComplete)
        {
            _logger.LogWarning("Recent Wakett prices are incomplete; continuing with weight calculation.");
        }

        try
        {
            await _weightCalculator.CalculateAndStoreAsync();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Weight calculation failed during automated schedule.");
        }
    }

    private async Task RunOrderSubmissionAsync(CancellationToken stoppingToken)
    {
        try
        {
            await _orderSender.SendOrdersAsync(ResolveAutomationAum(), stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Wakett order submission failed.");
        }
    }

    private async Task RunPnlWorkflowAsync(CancellationToken stoppingToken)
    {
        try
        {
            var report = await _pnlReportService.ComputeAndStoreCurrentDayPnlAsync(cancellationToken: stoppingToken);
            await _emailNotificationService.SendPnLReportAsync(report, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to compute or send automated Wakett PnL report.");
        }
    }

    private double? ResolveAutomationAum()
    {
        var configured = Environment.GetEnvironmentVariable("WAKETT_AUM")
            ?? _configuration["ExternalApis:WakettApi:Aum"];

        if (string.IsNullOrWhiteSpace(configured))
        {
            return null;
        }

        if (double.TryParse(configured, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed;
        }

        _logger.LogWarning("Unable to parse Wakett AUM value '{Value}'.", configured);
        return null;
    }

    private async Task RunFillCheckAsync(CancellationToken stoppingToken)
    {
        if (string.IsNullOrWhiteSpace(_options.FillAccount))
        {
            _logger.LogWarning(
                "Skipping Wakett fill check because Automation:Wakett:FillAccount is not configured.");
            return;
        }

        var localNow = TimeZoneInfo.ConvertTimeFromUtc(_timeProvider.GetUtcNow().UtcDateTime, NewYorkTimeZone);
        var tradingDate = DateOnly.FromDateTime(localNow);
        var dateString = tradingDate.ToString("yyyyMMdd", CultureInfo.InvariantCulture);

        var request = new FetchWakettFillsRequest
        {
            Account = _options.FillAccount,
            From = dateString,
            To = dateString,
            Strategy = _options.FillStrategy
        };

        _logger.LogInformation(
            "Requesting Wakett fills via automation for account {Account} covering {From} to {To} (strategy: {Strategy}).",
            request.Account,
            request.From,
            request.To,
            request.Strategy ?? "<all>");


        try
        {
            await _tradeFetcher.FetchAndStoreAsync(request, stoppingToken);
        }
        catch (TaskCanceledException ex) when (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogError(ex, "Wakett fill fetch timed out while calling the Wakett API.");
        }
        catch (WakettTradeFetcherException ex)
        {
            _logger.LogError(ex, "Wakett fill fetch returned an error: {Message}", ex.Message);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Unexpected failure while fetching Wakett fills.");
        }
    }

    private async Task DelayUntilAsync(DateTime targetUtc, CancellationToken stoppingToken)
    {
        var now = _timeProvider.GetUtcNow().UtcDateTime;
        var delay = targetUtc - now;
        if (delay <= TimeSpan.Zero)
        {
            return;
        }

        try
        {
            await Task.Delay(delay, stoppingToken);
        }
        catch (TaskCanceledException)
        {
        }
    }

    private DateTime GetSessionShutdownDeadlineUtc(DateTime referenceUtc)
    {
        var local = TimeZoneInfo.ConvertTimeFromUtc(referenceUtc, NewYorkTimeZone);
        var endLocal = GetSessionEndLocal(local).Add(SessionShutdownDelay);
        return TimeZoneInfo.ConvertTimeToUtc(endLocal, NewYorkTimeZone);
    }

    private static DateTime GetSessionEndLocal(DateTime local)
    {
        var end = new DateTime(local.Year, local.Month, local.Day, SessionEnd.Hours, SessionEnd.Minutes, SessionEnd.Seconds);
        if (SessionEnd < SessionStart)
        {
            end = end.AddDays(1);
        }

        return end;
    }

    private DateTime GetSessionEndUtc(DateTime sessionStartUtc)
    {
        var localStart = TimeZoneInfo.ConvertTimeFromUtc(sessionStartUtc, NewYorkTimeZone);
        var endLocal = GetSessionEndLocal(localStart);
        return TimeZoneInfo.ConvertTimeToUtc(endLocal, NewYorkTimeZone);
    }

    private DateTime GetNextSessionStartUtc(DateTime referenceUtc)
    {
        var local = TimeZoneInfo.ConvertTimeFromUtc(referenceUtc, NewYorkTimeZone);
        var candidate = new DateTime(local.Year, local.Month, local.Day, SessionStart.Hours, SessionStart.Minutes, 0);

        if (local < candidate && !IsWeekend(candidate))
        {
            return TimeZoneInfo.ConvertTimeToUtc(candidate, NewYorkTimeZone);
        }

        do
        {
            candidate = candidate.AddDays(1);
        }
        while (IsWeekend(candidate));

        return TimeZoneInfo.ConvertTimeToUtc(candidate, NewYorkTimeZone);
    }

    private bool IsWithinSession(DateTime utcTime)
    {
        var local = TimeZoneInfo.ConvertTimeFromUtc(utcTime, NewYorkTimeZone);
        if (IsWeekend(local))
        {
            return false;
        }

        var timeOfDay = local.TimeOfDay;
        if (SessionEnd < SessionStart)
        {
            return timeOfDay >= SessionStart || timeOfDay <= SessionEnd;
        }

        return timeOfDay >= SessionStart && timeOfDay <= SessionEnd;
    }

    private static bool IsWeekend(DateTime value)
        => value.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday;

    private static TimeZoneInfo NewYorkTimeZone
        => TimeZoneInfo.FindSystemTimeZoneById(
            OperatingSystem.IsWindows() ? "Eastern Standard Time" : "America/New_York");

    private DateTime GetNextSessionEventUtc(
        DateTime referenceUtc,
        IReadOnlyList<int> minuteOffsets,
        DateTime sessionStartUtc,
        DateTime sessionEndUtc)
    {
        if (minuteOffsets.Count == 0)
        {
            return DateTime.MaxValue;
        }

        var sessionStartLocal = TimeZoneInfo.ConvertTimeFromUtc(sessionStartUtc, NewYorkTimeZone);
        var sessionEndLocal = TimeZoneInfo.ConvertTimeFromUtc(sessionEndUtc, NewYorkTimeZone);
        var local = TimeZoneInfo.ConvertTimeFromUtc(referenceUtc, NewYorkTimeZone);

        if (local < sessionStartLocal)
        {
            local = sessionStartLocal;
        }

        if (local > sessionEndLocal)
        {
            return DateTime.MaxValue;
        }

        var currentHourStart = new DateTime(local.Year, local.Month, local.Day, local.Hour, 0, 0);

        while (currentHourStart <= sessionEndLocal)
        {
            foreach (var minute in minuteOffsets)
            {
                var candidateLocal = currentHourStart.AddMinutes(minute);
                if (candidateLocal < sessionStartLocal)
                {
                    continue;
                }

                if (candidateLocal > sessionEndLocal)
                {
                    continue;
                }

                if (candidateLocal < local)
                {
                    continue;
                }

                return TimeZoneInfo.ConvertTimeToUtc(candidateLocal, NewYorkTimeZone);
            }

            currentHourStart = currentHourStart.AddHours(1);
            local = currentHourStart;
        }

        return DateTime.MaxValue;
    }

    private static DateTime GetEarliest(params DateTime[] candidates)
    {
        var earliest = candidates[0];
        for (var i = 1; i < candidates.Length; i++)
        {
            if (candidates[i] < earliest)
            {
                earliest = candidates[i];
            }
        }

        return earliest;
    }
}
