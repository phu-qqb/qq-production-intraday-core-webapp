using System.Collections.Generic;
using System.Reflection;
using TradingDaemon.Services;
using Xunit;

public class UsdPairDedupTests
{
    [Fact]
    public void BuildUsdMap_RemovesInverseDuplicates()
    {
        var usdPairs = new List<(long SecurityId, string Ticker)>
        {
            (1, "AUDUSD Curncy"),
            (2, "USDAUD Curncy"),
            (3, "EURUSD Curncy")
        };

        var method = typeof(WeightCalculator).GetMethod("BuildUsdMap", BindingFlags.NonPublic | BindingFlags.Static);
        var usdMap = (Dictionary<(string Base, string Quote), long>)method!.Invoke(null, new object[] { usdPairs })!;

        var hasAudUsd = usdMap.ContainsKey(("AUD", "USD"));
        var hasUsdAud = usdMap.ContainsKey(("USD", "AUD"));
        Assert.True(hasAudUsd ^ hasUsdAud);
        Assert.Equal(2, usdMap.Count);
    }
}
