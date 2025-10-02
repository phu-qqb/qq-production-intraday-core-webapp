using Quartz;

namespace TradingDaemon.Services;

public class SchedulerService : IHostedService
{
    private readonly ISchedulerFactory _schedulerFactory;
    private IScheduler? _scheduler;

    public SchedulerService(ISchedulerFactory schedulerFactory)
    {
        _schedulerFactory = schedulerFactory;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _scheduler = await _schedulerFactory.GetScheduler(cancellationToken);

        var job = JobBuilder.Create<TradingJob>().WithIdentity("TradingJob").Build();
        var cron = "0 0/30 7-19 ? * *";
        var tz = TimeZoneInfo.FindSystemTimeZoneById("GMT Standard Time");
        var trigger = TriggerBuilder.Create()
            .WithSchedule(CronScheduleBuilder.CronSchedule(cron).InTimeZone(tz))
            .Build();

        await _scheduler.ScheduleJob(job, trigger, cancellationToken);
        await _scheduler.Start(cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_scheduler != null)
            await _scheduler.Shutdown(cancellationToken);
    }
}

public class TradingJob : IJob
{
    private readonly PriceFetcher _priceFetcher;
    private readonly WeightCalculator _weightCalculator;
    private readonly OrderSender _orderSender;
    private readonly PnlReportService _pnlReportService;

    public TradingJob(PriceFetcher priceFetcher, WeightCalculator weightCalculator, OrderSender orderSender, PnlReportService pnlReportService)
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
