using System;
using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using TradingDaemon.Models;
using TradingDaemon.Services;
using Xunit;

public sealed class WakettAutomationServiceTests
{
    private static TimeZoneInfo NewYorkTimeZone
        => TimeZoneInfo.FindSystemTimeZoneById(
            OperatingSystem.IsWindows() ? "Eastern Standard Time" : "America/New_York");

    [Fact]
    public void GetNextFillCheckAfter_AlignsToIntervalBoundaries()
    {
        var service = CreateService();
        var method = typeof(WakettAutomationService).GetMethod(
            "GetNextFillCheckAfter",
            BindingFlags.Instance | BindingFlags.NonPublic)!;

        var lastFillLocal = new DateTime(2024, 5, 1, 10, 2, 0);
        var lastFillUtc = TimeZoneInfo.ConvertTimeToUtc(lastFillLocal, NewYorkTimeZone);

        var nextFillUtc = (DateTime)method.Invoke(service, new object[] { lastFillUtc })!;
        var nextFillLocal = TimeZoneInfo.ConvertTimeFromUtc(nextFillUtc, NewYorkTimeZone);

        Assert.Equal(new DateTime(2024, 5, 1, 10, 10, 0), nextFillLocal);

        lastFillLocal = new DateTime(2024, 5, 1, 10, 10, 30);
        lastFillUtc = TimeZoneInfo.ConvertTimeToUtc(lastFillLocal, NewYorkTimeZone);

        nextFillUtc = (DateTime)method.Invoke(service, new object[] { lastFillUtc })!;
        nextFillLocal = TimeZoneInfo.ConvertTimeFromUtc(nextFillUtc, NewYorkTimeZone);

        Assert.Equal(new DateTime(2024, 5, 1, 10, 20, 0), nextFillLocal);
    }

    [Fact]
    public void GetNextFillCheckAfter_MovesToNextSessionWhenNeeded()
    {
        var service = CreateService();
        var method = typeof(WakettAutomationService).GetMethod(
            "GetNextFillCheckAfter",
            BindingFlags.Instance | BindingFlags.NonPublic)!;

        var lastFillLocal = new DateTime(2024, 5, 1, 15, 55, 0);
        var lastFillUtc = TimeZoneInfo.ConvertTimeToUtc(lastFillLocal, NewYorkTimeZone);

        var nextFillUtc = (DateTime)method.Invoke(service, new object[] { lastFillUtc })!;
        var nextFillLocal = TimeZoneInfo.ConvertTimeFromUtc(nextFillUtc, NewYorkTimeZone);

        Assert.Equal(new DateTime(2024, 5, 2, 9, 0, 0), nextFillLocal);
    }

    private static WakettAutomationService CreateService()
    {
        var options = Options.Create(new WakettAutomationOptions
        {
            FillIntervalMinutes = 10,
            WorkflowMinuteOffset = 8,
            FillAccount = "Test"
        });

        return new WakettAutomationService(
            Mock.Of<WakettPriceFetcher>(),
            Mock.Of<WeightCalculator>(),
            Mock.Of<OrderSender>(),
            Mock.Of<WakettTradeFetcher>(),
            Mock.Of<IHostApplicationLifetime>(),
            options,
            Mock.Of<ILogger<WakettAutomationService>>(),
            new ConfigurationBuilder().Build(),
            TimeProvider.System);
    }
}
