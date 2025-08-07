using Xunit;
using TradingDaemon.Services;

public class ProcessRunnerTests
{
    [Fact]
    public async Task RunAsync_ExecutesProcess()
    {
        var result = await ProcessRunner.RunAsync("bash", "-c \"echo test\"");
        Assert.Equal(0, result.ExitCode);
        Assert.Contains("test", result.StdOut);
    }
}
