using Xunit;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using TradingDaemon.Data;
using TradingDaemon.Services;

public class WeightCalculatorTests
{
    [Fact(Skip = "Requires GPU executable")]
    public async Task CalculateAndStoreAsync_ParsesOutput()
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["GpuExecutable"] = "bash"
        }).Build();
        var context = new DapperContext(config);
        var logger = Mock.Of<ILogger<WeightCalculator>>();
        var calc = new WeightCalculator(context, config, logger);
        await calc.CalculateAndStoreAsync();
    }
}
