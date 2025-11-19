using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using TradingDaemon.Data;
using TradingDaemon.Models;
using TradingDaemon.Services;
using Xunit;

public class PriceFetcherTests
{
    private class TestDapperContext : DapperContext
    {
        public TestDapperContext(IConfiguration config) : base(config) { }
        public override System.Data.IDbConnection CreateConnection()
            => throw new InvalidOperationException("Database access not expected in test");
    }

    [Fact]
    public async Task FetchAndStoreAsync_NoData_NoDbAccess()
    {
        var tempFile = Path.GetTempFileName();
        File.WriteAllText(tempFile, "Timestamp,123\n");

        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ConnectionStrings:DefaultConnection"] = "Server=localhost;Database=test;User Id=test;Password=test;",
            ["PriceCsvPath"] = tempFile
        }).Build();
        var context = new TestDapperContext(config);
        var logger = Mock.Of<ILogger<PriceFetcher>>();
        var provider = new DatabaseObjectNameProvider();
        var fetcher = new PriceFetcher(context, logger, config, provider);

        await fetcher.FetchAndStoreAsync();
    }

    [Fact]
    public void RawNMin_FirstUsBarAlignsToNextHour()
    {
        var series = new List<HistClose>
        {
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 13, 0, 0, DateTimeKind.Utc), Close = 1m },
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 14, 0, 0, DateTimeKind.Utc), Close = 2m },
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 15, 0, 0, DateTimeKind.Utc), Close = 3m }
        };

        var method = typeof(PriceFetcher).GetMethod("RawNMin", BindingFlags.NonPublic | BindingFlags.Static)!;
        var result = (List<(DateTime TimestampUtc, decimal Close)>)method.Invoke(null, new object[] { series, 60, "US", 0 })!;

        var zoneId = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Eastern Standard Time" : "America/New_York";
        var zone = TimeZoneInfo.FindSystemTimeZoneById(zoneId);
        var times = result.Select(r => TimeZoneInfo.ConvertTimeFromUtc(r.TimestampUtc, zone).TimeOfDay).ToList();

        Assert.Contains(new TimeSpan(10, 0, 0), times);
        Assert.DoesNotContain(new TimeSpan(9, 0, 0), times);

    }

    [Fact]
    public void RawNMin_FirstUsBarProducesTopOfHourTimestamp()
    {
        var series = new List<HistClose>
        {
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 14, 30, 0, DateTimeKind.Utc), Close = 1m },
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 15, 30, 0, DateTimeKind.Utc), Close = 2m },
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 16, 30, 0, DateTimeKind.Utc), Close = 3m }
        };

        var method = typeof(PriceFetcher).GetMethod("RawNMin", BindingFlags.NonPublic | BindingFlags.Static)!;
        var result = (List<(DateTime TimestampUtc, decimal Close)>)method.Invoke(null, new object[] { series, 60, "US", 0 })!;

        var zoneId = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Eastern Standard Time" : "America/New_York";
        var zone = TimeZoneInfo.FindSystemTimeZoneById(zoneId);
        var localTimes = result
            .Select(r => TimeZoneInfo.ConvertTimeFromUtc(r.TimestampUtc, zone))
            .ToList();

        Assert.Equal(new TimeSpan(10, 0, 0), localTimes.First().TimeOfDay);
        Assert.All(localTimes, t => Assert.Equal(0, t.Minute));

    }

    [Fact]
    public void RawNMin_UsesCETForEUSession()
    {
        var series = new List<HistClose>
        {
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 6, 6, 0, DateTimeKind.Utc), Close = 1m }, // 07:06 CET
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 6, 21, 0, DateTimeKind.Utc), Close = 2m }, // 07:21 CET
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 12, 36, 0, DateTimeKind.Utc), Close = 3m }, // 13:36 CET
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 12, 51, 0, DateTimeKind.Utc), Close = 4m }, // 13:51 CET
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 13, 6, 0, DateTimeKind.Utc), Close = 5m } // 14:06 CET (outside)
        };

        var method = typeof(PriceFetcher).GetMethod("RawNMin", BindingFlags.NonPublic | BindingFlags.Static)!;
        var result = (List<(DateTime TimestampUtc, decimal Close)>)method.Invoke(null, new object[] { series, 15, "EU", 0 })!;

        var zoneId = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Central European Standard Time" : "Europe/Berlin";
        var zone = TimeZoneInfo.FindSystemTimeZoneById(zoneId);
        var times = result.Select(r => TimeZoneInfo.ConvertTimeFromUtc(r.TimestampUtc, zone).TimeOfDay).ToList();

        Assert.Contains(new TimeSpan(7, 6, 0), times);
        Assert.Contains(new TimeSpan(13, 51, 0), times);
        Assert.DoesNotContain(new TimeSpan(6, 51, 0), times);
        Assert.DoesNotContain(new TimeSpan(14, 6, 0), times);
    }

    [Fact]
    public void Flatten_FiltersBarsToEUSession()
    {
        var series = new List<HistClose>
        {
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 5, 51, 0, DateTimeKind.Utc), Close = 1m }, // 06:51 CET (pre session)
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 6, 6, 0, DateTimeKind.Utc), Close = 2m }, // 07:06 CET (start)
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 6, 21, 0, DateTimeKind.Utc), Close = 3m },
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 12, 36, 0, DateTimeKind.Utc), Close = 4m },
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 12, 51, 0, DateTimeKind.Utc), Close = 5m }, // 13:51 CET (end)
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 13, 6, 0, DateTimeKind.Utc), Close = 6m } // 14:06 CET (post session)
        };

        var raw = (List<(DateTime TimestampUtc, decimal Close)>)typeof(PriceFetcher)
            .GetMethod("RawNMin", BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, new object[] { series, 15, "EU", 0 })!;

        var zoneId = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? "Central European Standard Time"
            : "Europe/Berlin";
        var zone = TimeZoneInfo.FindSystemTimeZoneById(zoneId);

        var flat = (List<(DateTime TimestampUtc, decimal Close)>)typeof(PriceFetcher)
            .GetMethod("Flatten", BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, new object[] { raw, zone })!;

        var times = flat.Select(r => TimeZoneInfo.ConvertTimeFromUtc(r.TimestampUtc, zone).TimeOfDay).ToList();

        Assert.DoesNotContain(new TimeSpan(6, 51, 0), times);
        Assert.DoesNotContain(new TimeSpan(14, 6, 0), times);
        Assert.Contains(new TimeSpan(7, 6, 0), times);
        Assert.Contains(new TimeSpan(13, 51, 0), times);
    }

    [Fact]
    public void Flatten_FiltersBarsToUSSession()
    {
        var series = new List<HistClose>
        {
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 13, 0, 0, DateTimeKind.Utc), Close = 1m },
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 14, 0, 0, DateTimeKind.Utc), Close = 2m },
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 20, 0, 0, DateTimeKind.Utc), Close = 3m },
            new HistClose { BarTimeUtc = new DateTime(2024, 1, 2, 21, 0, 0, DateTimeKind.Utc), Close = 4m }
        };

        var raw = (List<(DateTime TimestampUtc, decimal Close)>)typeof(PriceFetcher)
            .GetMethod("RawNMin", BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, new object[] { series, 60, "US", 0 })!;

        var zoneId = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? "Eastern Standard Time"
            : "America/New_York";
        var zone = TimeZoneInfo.FindSystemTimeZoneById(zoneId);

        var flat = (List<(DateTime TimestampUtc, decimal Close)>)typeof(PriceFetcher)
            .GetMethod("Flatten", BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, new object[] { raw, zone })!;

        var times = flat.Select(r => TimeZoneInfo.ConvertTimeFromUtc(r.TimestampUtc, zone).TimeOfDay).ToList();

        Assert.DoesNotContain(new TimeSpan(8, 0, 0), times);
        Assert.DoesNotContain(new TimeSpan(16, 0, 0), times);
        Assert.Contains(new TimeSpan(10, 0, 0), times);
        Assert.Contains(new TimeSpan(15, 0, 0), times);
    }

    [Fact]
    public void Flatten_ZeroesOvernightReturnsAndPreservesIntraday()
    {
        var raw = new List<(DateTime TimestampUtc, decimal Close)>
        {
            (new DateTime(2024, 1, 1, 10, 0, 0, DateTimeKind.Utc), 100m),
            (new DateTime(2024, 1, 1, 11, 0, 0, DateTimeKind.Utc), 105m),
            (new DateTime(2024, 1, 2, 10, 0, 0, DateTimeKind.Utc), 110m),
            (new DateTime(2024, 1, 2, 11, 0, 0, DateTimeKind.Utc), 121m)
        };

        var zone = TimeZoneInfo.Utc;

        var flat = (List<(DateTime TimestampUtc, decimal Close)>)typeof(PriceFetcher)
            .GetMethod("Flatten", BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, new object[] { raw, zone })!;

        static decimal ReturnBetween((DateTime TimestampUtc, decimal Close) prev, (DateTime TimestampUtc, decimal Close) next)
            => prev.Close != 0 ? (next.Close - prev.Close) / prev.Close : 0m;

        var expectedIntradayFirst = ReturnBetween(raw[0], raw[1]);
        var expectedIntradaySecond = ReturnBetween(raw[2], raw[3]);

        var flattenedFirst = ReturnBetween(flat[0], flat[1]);
        var flattenedOvernight = ReturnBetween(flat[1], flat[2]);
        var flattenedSecond = ReturnBetween(flat[2], flat[3]);

        Assert.Equal(expectedIntradayFirst, flattenedFirst);
        Assert.Equal(0m, flattenedOvernight);
        Assert.Equal(expectedIntradaySecond, flattenedSecond);
    }
}
