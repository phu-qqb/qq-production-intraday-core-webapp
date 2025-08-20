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
            ["Executables:GenBinariesExecutable"] = "bash",
            ["Executables:PythonExecutable"] = "python3",
            ["Programmes:0:Universe"] = "INFXUS",
            ["Programmes:0:Session"] = "US",
            ["Programmes:0:Timeframe"] = "60",
            ["Programmes:0:StartDate"] = "2022-01-01T00:00:00Z"
        }).Build();
        var context = new DapperContext(config);
        var logger = Mock.Of<ILogger<WeightCalculator>>();
        var calc = new WeightCalculator(context, config, logger);
        await calc.CalculateAndStoreAsync();
    }
}
