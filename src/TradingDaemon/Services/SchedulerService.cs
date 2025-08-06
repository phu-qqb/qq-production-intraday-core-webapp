using Quartz;

namespace TradingDaemon.Services;

public class SchedulerService : IHostedService
{
    private readonly ISchedulerFactory _schedulerFactory;
    private readonly IServiceProvider _provider;
    private IScheduler? _scheduler;

    public SchedulerService(ISchedulerFactory schedulerFactory, IServiceProvider provider)
    {
        _schedulerFactory = schedulerFactory;
        _provider = provider;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
        _scheduler.JobFactory = new MicrosoftDependencyInjectionJobFactory(_provider);

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

    public TradingJob(PriceFetcher priceFetcher, WeightCalculator weightCalculator, OrderSender orderSender)
    {
        _priceFetcher = priceFetcher;
        _weightCalculator = weightCalculator;
        _orderSender = orderSender;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        await _priceFetcher.FetchAndStoreAsync();
        await _weightCalculator.CalculateAndStoreAsync();
        await _orderSender.SendOrdersAsync();
    }
}
