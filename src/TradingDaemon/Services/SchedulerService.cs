using System;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Quartz;
using Quartz.Spi;
using TradingDaemon.Options;

namespace TradingDaemon.Services;

public class SchedulerService : IHostedService
{
    private readonly ISchedulerFactory _schedulerFactory;
    private readonly IJobFactory _jobFactory;
    private readonly IOptionsMonitor<SchedulerOptions> _optionsMonitor;
    private readonly ILogger<SchedulerService> _logger;
    private IScheduler? _scheduler;

    public SchedulerService(
        ISchedulerFactory schedulerFactory,
        IJobFactory jobFactory,
        IOptionsMonitor<SchedulerOptions> optionsMonitor,
        ILogger<SchedulerService> logger)
    {
        _schedulerFactory = schedulerFactory;
        _jobFactory = jobFactory;
        _optionsMonitor = optionsMonitor;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
        _scheduler.JobFactory = _jobFactory;

        var schedulerOptions = _optionsMonitor.CurrentValue;
        var cron = string.IsNullOrWhiteSpace(schedulerOptions.Cron)
            ? "0 0/30 7-19 ? * *"
            : schedulerOptions.Cron;
        var timeZoneId = string.IsNullOrWhiteSpace(schedulerOptions.TimeZone)
            ? TimeZoneInfo.Utc.Id
            : schedulerOptions.TimeZone;

        var timeZone = ResolveTimeZone(timeZoneId);

        var job = JobBuilder.Create<TradingJob>().WithIdentity("TradingJob").Build();
        var trigger = TriggerBuilder.Create()
            .WithIdentity("TradingJobTrigger")
            .WithSchedule(CronScheduleBuilder.CronSchedule(cron).InTimeZone(timeZone))
            .Build();

        await _scheduler.ScheduleJob(job, trigger, cancellationToken);
        await _scheduler.Start(cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_scheduler != null)
            await _scheduler.Shutdown(cancellationToken);
    }

    private TimeZoneInfo ResolveTimeZone(string timeZoneId)
    {
        try
        {
            return TimeZoneInfo.FindSystemTimeZoneById(timeZoneId);
        }
        catch (TimeZoneNotFoundException ex) when (TryResolveAlternate(timeZoneId, out var timeZone))
        {
            _logger.LogWarning(ex, "Falling back to alternate time zone ID {Fallback} for configured ID {Configured}", timeZone.Id, timeZoneId);
            return timeZone;
        }
        catch (InvalidTimeZoneException ex) when (TryResolveAlternate(timeZoneId, out var timeZone))
        {
            _logger.LogWarning(ex, "Falling back to alternate time zone ID {Fallback} for configured ID {Configured}", timeZone.Id, timeZoneId);
            return timeZone;
        }
    }

    private static bool TryResolveAlternate(string timeZoneId, out TimeZoneInfo timeZone)
    {
        if (!TimeZoneInfo.TryConvertWindowsIdToIanaId(timeZoneId, out var ianaId))
        {
            timeZone = null!;
            return false;
        }

        try
        {
            timeZone = TimeZoneInfo.FindSystemTimeZoneById(ianaId);
            return true;
        }
        catch (TimeZoneNotFoundException)
        {
            timeZone = null!;
            return false;
        }
        catch (InvalidTimeZoneException)
        {
            timeZone = null!;
            return false;
        }
    }
}

public class TradingJob : IJob
{
    private readonly PriceFetcher _priceFetcher;
    private readonly WeightCalculator _weightCalculator;
    private readonly OrderSender _orderSender;
    private readonly PnlReportService _pnlReportService;

    public TradingJob(
        PriceFetcher priceFetcher,
        WeightCalculator weightCalculator,
        OrderSender orderSender,
        PnlReportService pnlReportService)
    {
        _priceFetcher = priceFetcher;
        _weightCalculator = weightCalculator;
        _orderSender = orderSender;
        _pnlReportService = pnlReportService;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        await _priceFetcher.FetchAndStoreAsync();
        await _pnlReportService.ComputeAndStoreCurrentDayPnlAsync(cancellationToken: context.CancellationToken);

        await _weightCalculator.CalculateAndStoreAsync();
        await _orderSender.SendOrdersAsync();
    }
}
