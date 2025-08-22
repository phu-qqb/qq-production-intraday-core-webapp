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
        var fetcher = new PriceFetcher(context, logger, config);

        await fetcher.FetchAndStoreAsync();
    }

    [Fact]
    public void RawNMin_Includes9amBarInUSSession()
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

        Assert.Contains(new TimeSpan(9, 0, 0), times);
    }
}
