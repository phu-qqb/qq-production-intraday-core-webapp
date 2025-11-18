using System.Linq;
using TradingDaemon.Services;
using Xunit;

public class FlatBarBuildSpecificationFactoryTests
{
    [Fact]
    public void CreateDefault_IncludesBaseAndAdditionalSchedules()
    {
        var builds = FlatBarBuildSpecificationFactory.CreateDefault(15, 0);

        Assert.Equal(3, builds.Count);
        Assert.Contains(builds, b => b.TimeframeMinute == 15 && b.OffsetMinute == 0);
        Assert.Contains(builds, b => b.TimeframeMinute == 30 && b.OffsetMinute == 6);
        Assert.Contains(builds, b => b.TimeframeMinute == 60 && b.OffsetMinute == 6);
    }

    [Fact]
    public void CreateDefault_DeduplicatesEquivalentBuilds()
    {
        var builds = FlatBarBuildSpecificationFactory.CreateDefault(30, 66);

        Assert.Equal(2, builds.Count);
        Assert.Single(builds.Where(b => b.TimeframeMinute == 30 && b.OffsetMinute == 6));
        Assert.Single(builds.Where(b => b.TimeframeMinute == 60 && b.OffsetMinute == 6));
    }
}
