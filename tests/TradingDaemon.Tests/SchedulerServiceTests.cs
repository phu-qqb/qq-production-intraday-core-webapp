using Moq;
using Quartz;
using TradingDaemon.Services;
using Microsoft.Extensions.DependencyInjection;

public class SchedulerServiceTests
{
    [Fact(Skip = "Requires Quartz scheduler")]
    public async Task StartAsync_SchedulesJob()
    {
        var schedulerFactory = new Mock<ISchedulerFactory>();
        var scheduler = new Mock<IScheduler>();
        schedulerFactory.Setup(f => f.GetScheduler(It.IsAny<CancellationToken>())).ReturnsAsync(scheduler.Object);
        var service = new SchedulerService(schedulerFactory.Object, new ServiceCollection().BuildServiceProvider());

        await service.StartAsync(CancellationToken.None);

        scheduler.Verify(s => s.ScheduleJob(It.IsAny<IJobDetail>(), It.IsAny<ITrigger>(), It.IsAny<CancellationToken>()), Times.Once);
    }
}
