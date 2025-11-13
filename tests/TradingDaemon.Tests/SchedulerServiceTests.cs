using Xunit;
using Moq;
using Quartz;
using Quartz.Spi;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TradingDaemon.Options;
using TradingDaemon.Services;

public class SchedulerServiceTests
{
    [Fact(Skip = "Requires Quartz scheduler")]
    public async Task StartAsync_SchedulesJob()
    {
        var schedulerFactory = new Mock<ISchedulerFactory>();
        var scheduler = new Mock<IScheduler>();
        schedulerFactory.Setup(f => f.GetScheduler(It.IsAny<CancellationToken>())).ReturnsAsync(scheduler.Object);

        var jobFactory = new Mock<IJobFactory>();
        var optionsMonitor = Mock.Of<IOptionsMonitor<SchedulerOptions>>(o => o.CurrentValue == new SchedulerOptions
        {
            Cron = "0 0/30 * * * ?",
            TimeZone = "UTC"
        });
        var logger = Mock.Of<ILogger<SchedulerService>>();

        var service = new SchedulerService(schedulerFactory.Object, jobFactory.Object, optionsMonitor, logger);

        await service.StartAsync(CancellationToken.None);

        scheduler.Verify(s => s.ScheduleJob(It.IsAny<IJobDetail>(), It.IsAny<ITrigger>(), It.IsAny<CancellationToken>()), Times.Once);
    }
}
