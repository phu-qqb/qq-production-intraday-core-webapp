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
    private static readonly TimeSpan SessionShutdownDelay = TimeSpan.FromHours(1);

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

        try
        {
            await RunTradingWorkflowAsync(stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Initial Wakett workflow run failed.");
        }

        var sessionActive = false;
        DateTime? sessionShutdownDeadlineUtc = null;
        var postSessionFillCheckCompleted = false;
        var postSessionWorkflowCompleted = false;
        var initialReferenceUtc = _timeProvider.GetUtcNow().UtcDateTime;
        var nextWorkflowUtc = GetNextWorkflowRunUtc(initialReferenceUtc);
        var nextFillUtc = GetNextFillCheckUtc(initialReferenceUtc);

        _logger.LogInformation(
            "Initial Wakett automation schedule set: next workflow at {WorkflowUtc:o}, next fill check at {FillUtc:o}.",
            nextWorkflowUtc,
            nextFillUtc);

        while (!stoppingToken.IsCancellationRequested)
        {
            var nowUtc = _timeProvider.GetUtcNow().UtcDateTime;
            var withinSession = IsWithinSession(nowUtc);

            if (!withinSession && sessionActive)
            {
                sessionShutdownDeadlineUtc ??= GetSessionShutdownDeadlineUtc(nowUtc);
            }

            var canRunWorkflow = withinSession
                || (sessionActive
                    && !postSessionWorkflowCompleted
                    && sessionShutdownDeadlineUtc is { } shutdownDeadline
                    && nowUtc < shutdownDeadline);

            if (canRunWorkflow && nowUtc >= nextWorkflowUtc)
            {
                try
                {
                    await RunTradingWorkflowAsync(stoppingToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogError(ex, "Scheduled Wakett workflow run failed.");
                }

                var reference = _timeProvider.GetUtcNow().UtcDateTime.AddSeconds(1);

                if (withinSession)
                {
                    nextWorkflowUtc = GetNextWorkflowRunUtc(reference);
                    _logger.LogDebug(
                        "Rescheduled next Wakett workflow run for {WorkflowUtc:o}.",
                        nextWorkflowUtc);
                }
                else
                {
                    postSessionWorkflowCompleted = true;
                    nextWorkflowUtc = GetFirstWorkflowRunUtc(GetNextSessionStartUtc(reference));
                    _logger.LogInformation(
                        "Completed post-session Wakett workflow. Next run scheduled for {WorkflowUtc:o}.",
                        nextWorkflowUtc);
                }
            }

            if (withinSession)
            {
                sessionShutdownDeadlineUtc = null;
                postSessionFillCheckCompleted = false;
                postSessionWorkflowCompleted = false;
                if (!sessionActive)
                {
                    sessionActive = true;
                    _logger.LogInformation(
                        "Entering Wakett session window at {NowUtc:o}; triggering immediate fill check.",
                        nowUtc);
                    await RunFillCheckAsync(stoppingToken);
                    nextWorkflowUtc = GetNextWorkflowRunUtc(nowUtc);
                    nextFillUtc = GetNextFillCheckUtc(_timeProvider.GetUtcNow().UtcDateTime.AddSeconds(1));
                    _logger.LogInformation(
                        "Next scheduled Wakett workflow at {WorkflowUtc:o}; next fill check at {FillUtc:o}.",
                        nextWorkflowUtc,
                        nextFillUtc);
                }

                if (nowUtc >= nextFillUtc)
                {
                    await RunFillCheckAsync(stoppingToken);
                    nextFillUtc = GetNextFillCheckUtc(_timeProvider.GetUtcNow().UtcDateTime.AddSeconds(1));
                    _logger.LogInformation(
                        "Next Wakett fill check scheduled for {FillUtc:o}.",
                        nextFillUtc);
                }

                var nextEvent = nextWorkflowUtc < nextFillUtc ? nextWorkflowUtc : nextFillUtc;
                var sessionEndUtc = GetCurrentSessionEndUtc(nowUtc);
                if (nextEvent > sessionEndUtc)
                {
                    nextEvent = sessionEndUtc;
                }
                await DelayUntilAsync(nextEvent, stoppingToken);
            }
            else
            {
                if (sessionActive)
                {
                    sessionShutdownDeadlineUtc ??= GetSessionShutdownDeadlineUtc(nowUtc);

                    if (!postSessionFillCheckCompleted)
                    {
                        await RunFillCheckAsync(stoppingToken);
                        postSessionFillCheckCompleted = true;
                        _logger.LogInformation(
                            "US session completed. Reports will continue until {ShutdownUtc:o} before shutdown.",
                            sessionShutdownDeadlineUtc);
                    }

                    if (nowUtc >= sessionShutdownDeadlineUtc)
                    {
                        _logger.LogInformation("Post-session buffer elapsed. Shutting down application.");
                        _applicationLifetime.StopApplication();
                        return;
                    }

                    var nextDelayTarget = sessionShutdownDeadlineUtc.Value;
                    if (!postSessionWorkflowCompleted && nextWorkflowUtc < nextDelayTarget)
                    {
                        nextDelayTarget = nextWorkflowUtc;
                    }

                    await DelayUntilAsync(nextDelayTarget, stoppingToken);
                    continue;
                }

                var nextSessionStart = GetNextSessionStartUtc(nowUtc);
                nextWorkflowUtc = GetFirstWorkflowRunUtc(nextSessionStart);
                nextFillUtc = GetFirstFillCheckUtc(nextSessionStart);
                _logger.LogInformation(
                    "Outside Wakett session window at {NowUtc:o}; next session starts at {SessionStartUtc:o}.",
                    nowUtc,
                    nextSessionStart);
                _logger.LogInformation(
                    "Next workflow scheduled for {WorkflowUtc:o} with fill check at {FillUtc:o} once the session begins.",
                    nextWorkflowUtc,
                    nextFillUtc);
                await DelayUntilAsync(nextSessionStart, stoppingToken);
            }
        }
    }

    private async Task RunTradingWorkflowAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Executing automated Wakett trading workflow.");

        WakettPriceUploadResult? uploadResult = null;
        try
        {
            uploadResult = await _priceFetcher.FetchAndStoreAsync(stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed to fetch Wakett prices.");
        }

        var hasNewPrices = uploadResult?.Prices.Count > 0;

        if (hasNewPrices)
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
                _logger.LogError(ex, "Failed to compute or send PnL report after Wakett price update.");
            }
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
            _logger.LogWarning(
                "Recent Wakett prices are incomplete; continuing automation workflow.");
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
            await WaitForNextHourPlusOneAsync(stoppingToken);
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
            var minimumDelayMinutes = Math.Max(WorkflowMinuteOffset, 1);
            var postSessionLocal = endLocal.AddMinutes(minimumDelayMinutes);
            var shutdownLocal = endLocal.Add(SessionShutdownDelay);

            if (postSessionLocal > shutdownLocal)
            {
                postSessionLocal = shutdownLocal;
            }

            if (postSessionLocal <= local)
            {
                var nextSession = GetNextSessionStartUtc(referenceUtc);
                return GetFirstWorkflowRunUtc(nextSession);
            }

            return TimeZoneInfo.ConvertTimeToUtc(postSessionLocal, NewYorkTimeZone);
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

        var interval = Math.Max(1, _options.FillIntervalMinutes);
        var local = TimeZoneInfo.ConvertTimeFromUtc(referenceUtc, NewYorkTimeZone);
        var endLocal = GetSessionEndLocal(local);
        var minute = local.Minute;
        var remainder = minute % interval;
        var delta = remainder == 0 && local.Second == 0 ? interval : interval - remainder;
        var candidate = new DateTime(local.Year, local.Month, local.Day, local.Hour, minute, 0).AddMinutes(delta);

        if (candidate > endLocal)
        {
            var nextSession = GetNextSessionStartUtc(referenceUtc);
            return GetFirstFillCheckUtc(nextSession);
        }

        return TimeZoneInfo.ConvertTimeToUtc(candidate, NewYorkTimeZone);
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

    private DateTime GetFirstFillCheckUtc(DateTime sessionStartUtc)
    {
        return sessionStartUtc;
    }

    private DateTime GetSessionShutdownDeadlineUtc(DateTime referenceUtc)
    {
        var local = TimeZoneInfo.ConvertTimeFromUtc(referenceUtc, NewYorkTimeZone);
        var endLocal = GetSessionEndLocal(local).Add(SessionShutdownDelay);
        return TimeZoneInfo.ConvertTimeToUtc(endLocal, NewYorkTimeZone);
    }

    private DateTime GetCurrentSessionEndUtc(DateTime referenceUtc)
    {
        var local = TimeZoneInfo.ConvertTimeFromUtc(referenceUtc, NewYorkTimeZone);
        var endLocal = GetSessionEndLocal(local);
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

    private int WorkflowMinuteOffset
    {
        get
        {
            var configuredOffset = Math.Clamp(_options.WorkflowMinuteOffset, 0, 59);
            var priceOffset = PriceMinuteOffset;

            if (configuredOffset < priceOffset)
            {
                _logger.LogDebug(
                    "Adjusting Wakett workflow offset from {Configured} to {PriceOffset} to ensure prices are available before triggering.",
                    configuredOffset,
                    priceOffset);
            }

            return Math.Max(configuredOffset, priceOffset);
        }
    }

    private int PriceMinuteOffset
        => Math.Clamp(_configuration.GetValue<int?>("ExternalApis:WakettApi:PriceMinuteOffset") ?? 6, 0, 59);

    private static TimeZoneInfo NewYorkTimeZone
        => TimeZoneInfo.FindSystemTimeZoneById(
            OperatingSystem.IsWindows() ? "Eastern Standard Time" : "America/New_York");

    private async Task WaitForNextHourPlusOneAsync(CancellationToken stoppingToken)
    {
        var nowUtc = _timeProvider.GetUtcNow().UtcDateTime;
        var local = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, NewYorkTimeZone);
        var nextHour = new DateTime(local.Year, local.Month, local.Day, local.Hour, 0, 0).AddHours(1);
        var targetLocal = nextHour.AddMinutes(1);
        var targetUtc = TimeZoneInfo.ConvertTimeToUtc(targetLocal, NewYorkTimeZone);
        await DelayUntilAsync(targetUtc, stoppingToken);
    }
}
