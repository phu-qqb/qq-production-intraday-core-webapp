using System;
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

    private readonly WakettPriceFetcher _priceFetcher;
    private readonly WeightCalculator _weightCalculator;
    private readonly OrderSender _orderSender;
    private readonly WakettTradeFetcher _tradeFetcher;
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
        _applicationLifetime = applicationLifetime;
        _logger = logger;
        _options = options?.Value ?? new WakettAutomationOptions();
        _configuration = configuration;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting Wakett automation service.");

        try
        {
            await RunTradingWorkflowAsync(stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Initial Wakett workflow run failed.");
        }

        var sessionActive = false;
        var nextWorkflowUtc = GetNextWorkflowRunUtc(_timeProvider.GetUtcNow().UtcDateTime);
        var nextFillUtc = GetNextFillCheckUtc(_timeProvider.GetUtcNow().UtcDateTime);

        while (!stoppingToken.IsCancellationRequested)
        {
            var nowUtc = _timeProvider.GetUtcNow().UtcDateTime;
            var withinSession = IsWithinSession(nowUtc);

            if (withinSession)
            {
                if (!sessionActive)
                {
                    sessionActive = true;
                    await RunFillCheckAsync(stoppingToken);
                    nextWorkflowUtc = GetNextWorkflowRunUtc(nowUtc);
                    var fillCompletedUtc = _timeProvider.GetUtcNow().UtcDateTime;
                    nextFillUtc = GetNextFillCheckAfter(fillCompletedUtc);
                }

                if (nowUtc >= nextWorkflowUtc)
                {
                    try
                    {
                        await RunTradingWorkflowAsync(stoppingToken);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger.LogError(ex, "Scheduled Wakett workflow run failed.");
                    }

                    nextWorkflowUtc = GetNextWorkflowRunUtc(_timeProvider.GetUtcNow().UtcDateTime.AddSeconds(1));
                }

                if (nowUtc >= nextFillUtc)
                {
                    await RunFillCheckAsync(stoppingToken);
                    var fillCompletedUtc = _timeProvider.GetUtcNow().UtcDateTime;
                    nextFillUtc = GetNextFillCheckAfter(fillCompletedUtc);
                }

                var nextEvent = nextWorkflowUtc < nextFillUtc ? nextWorkflowUtc : nextFillUtc;
                await DelayUntilAsync(nextEvent, stoppingToken);
            }
            else
            {
                if (sessionActive)
                {
                    await RunFillCheckAsync(stoppingToken);
                    _logger.LogInformation("US session completed. Shutting down application.");
                    _applicationLifetime.StopApplication();
                    return;
                }

                var nextSessionStart = GetNextSessionStartUtc(nowUtc);
                nextWorkflowUtc = GetFirstWorkflowRunUtc(nextSessionStart);
                nextFillUtc = GetFirstFillCheckUtc(nextSessionStart);
                await DelayUntilAsync(nextSessionStart, stoppingToken);
            }
        }
    }

    private async Task RunTradingWorkflowAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Executing automated Wakett trading workflow.");

        try
        {
            await _priceFetcher.FetchAndStoreAsync(stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed to fetch Wakett prices.");
        }

        bool pricesComplete;
        try
        {
            pricesComplete = await _priceFetcher.AreRecentPricesCompleteAsync(stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed to verify Wakett prices completeness.");
            return;
        }

        if (!pricesComplete)
        {
            _logger.LogWarning("Skipping weight calculation and order submission because prices are incomplete.");
            return;
        }

        try
        {
            await _weightCalculator.CalculateAndStoreAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Weight calculation failed; skipping order submission.");
            return;
        }

        try
        {
            await _orderSender.SendOrdersAsync(ResolveAutomationAum(), stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Wakett order submission failed.");
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
            _logger.LogDebug("Skipping Wakett fill check because no account is configured.");
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

        try
        {
            await _tradeFetcher.FetchAndStoreAsync(request, stoppingToken);
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

    private DateTime GetNextWorkflowRunUtc(DateTime referenceUtc)
    {
        if (!IsWithinSession(referenceUtc))
        {
            var nextSession = GetNextSessionStartUtc(referenceUtc);
            return GetFirstWorkflowRunUtc(nextSession);
        }

        var local = TimeZoneInfo.ConvertTimeFromUtc(referenceUtc, NewYorkTimeZone);
        var endLocal = GetSessionEndLocal(local);
        var candidate = new DateTime(local.Year, local.Month, local.Day, local.Hour, WorkflowMinuteOffset, 0);
        if (candidate < local)
        {
            candidate = candidate.AddHours(1);
        }

        if (candidate > endLocal)
        {
            var nextSession = GetNextSessionStartUtc(referenceUtc);
            return GetFirstWorkflowRunUtc(nextSession);
        }

        return TimeZoneInfo.ConvertTimeToUtc(candidate, NewYorkTimeZone);
    }

    private DateTime GetNextFillCheckUtc(DateTime referenceUtc)
    {
        if (!IsWithinSession(referenceUtc))
        {
            var nextSession = GetNextSessionStartUtc(referenceUtc);
            return GetFirstFillCheckUtc(nextSession);
        }

        return GetNextFillCheckAfter(referenceUtc);
    }

    private DateTime GetFirstWorkflowRunUtc(DateTime sessionStartUtc)
    {
        var localStart = TimeZoneInfo.ConvertTimeFromUtc(sessionStartUtc, NewYorkTimeZone);
        var firstLocal = localStart.AddMinutes(WorkflowMinuteOffset);
        var endLocal = GetSessionEndLocal(localStart);
        if (firstLocal > endLocal)
        {
            firstLocal = localStart;
        }

        return TimeZoneInfo.ConvertTimeToUtc(firstLocal, NewYorkTimeZone);
    }

    private DateTime GetNextFillCheckAfter(DateTime lastFillUtc)
    {
        var interval = Math.Max(1, _options.FillIntervalMinutes);
        var lastFillLocal = TimeZoneInfo.ConvertTimeFromUtc(lastFillUtc, NewYorkTimeZone);

        var baseMinute = lastFillLocal.Minute - (lastFillLocal.Minute % interval);
        var scheduledLocal = new DateTime(
            lastFillLocal.Year,
            lastFillLocal.Month,
            lastFillLocal.Day,
            lastFillLocal.Hour,
            baseMinute,
            0);

        if (scheduledLocal <= lastFillLocal)
        {
            scheduledLocal = scheduledLocal.AddMinutes(interval);
        }

        var candidateUtc = TimeZoneInfo.ConvertTimeToUtc(scheduledLocal, NewYorkTimeZone);

        if (!IsWithinSession(candidateUtc))
        {
            var nextSession = GetNextSessionStartUtc(lastFillUtc);
            return GetFirstFillCheckUtc(nextSession);
        }

        return candidateUtc;
    }

    private DateTime GetFirstFillCheckUtc(DateTime sessionStartUtc)
    {
        return sessionStartUtc;
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

    private int WorkflowMinuteOffset => Math.Clamp(_options.WorkflowMinuteOffset, 0, 59);

    private static TimeZoneInfo NewYorkTimeZone
        => TimeZoneInfo.FindSystemTimeZoneById(
            OperatingSystem.IsWindows() ? "Eastern Standard Time" : "America/New_York");
}
