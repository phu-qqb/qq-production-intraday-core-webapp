using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using TradingDaemon.Data;
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
}
