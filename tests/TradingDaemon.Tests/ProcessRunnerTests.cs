using System.Collections.Generic;
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

    [Fact]
    public async Task RunAsync_StreamsOutput()
    {
        var outs = new List<string>();
        var errs = new List<string>();
        var result = await ProcessRunner.RunAsync("bash", "-c \"echo out; echo err >&2\"", outs.Add, errs.Add);

        Assert.Equal(0, result.ExitCode);
        Assert.Contains("out", outs);
        Assert.Contains("err", errs);
    }
}
