using System;
using System.Collections.Generic;
using System.Reflection;
using TradingDaemon.Services;
using Xunit;

public class UsdPairNormalizationTests
{
    [Fact]
    public void BuildUsdMap_UsesUsdAsQuote()
    {
        var usdPairs = new List<(long SecurityId, string Ticker)>
        {
            (1, "AUDUSD Curncy"),
            (2, "USDAUD Curncy"),
            (3, "EURUSD Curncy"),
        };

        var method = typeof(WeightCalculator).GetMethod("BuildUsdMap", BindingFlags.NonPublic | BindingFlags.Static);
        var usdMap = (Dictionary<(string Base, string Quote), long>)method!.Invoke(null, new object[] { usdPairs })!;

        Assert.Equal(2, usdMap.Count);
        Assert.Equal(1L, usdMap[("AUD", "USD")]);
        Assert.Equal(3L, usdMap[("EUR", "USD")]);
    }

    [Fact]
    public void NormalizeUsdPair_MergesInversePairs()
    {
        var usdPairs = new List<(long SecurityId, string Ticker)>
        {
            (1, "USDCHF Curncy"),
            (2, "CHFUSD Curncy"),
        };

        var buildMethod = typeof(WeightCalculator).GetMethod("BuildUsdMap", BindingFlags.NonPublic | BindingFlags.Static);
        var usdMap = (Dictionary<(string Base, string Quote), long>)buildMethod!.Invoke(null, new object[] { usdPairs })!;

        var normMethod = typeof(WeightCalculator).GetMethod("NormalizeUsdPair", BindingFlags.NonPublic | BindingFlags.Static);

        var result1 = ((long SecurityId, decimal Weight))normMethod!.Invoke(null, new object[] { 1L, "USDCHF", 1m, usdMap })!;
        var result2 = ((long SecurityId, decimal Weight))normMethod!.Invoke(null, new object[] { 2L, "CHFUSD", 2m, usdMap })!;

        Assert.Equal(2L, result1.SecurityId);
        Assert.Equal(-1m, result1.Weight);
        Assert.Equal(2L, result2.SecurityId);
        Assert.Equal(2m, result2.Weight);
        Assert.Equal(1m, result1.Weight + result2.Weight);
    }

    [Fact]
    public void AdjustWeightForUsdQuote_FlipsSign()
    {
        var method = typeof(WeightCalculator).GetMethod(
            "AdjustWeightForUsdQuote", BindingFlags.NonPublic | BindingFlags.Static);
        var usdQuoteIds = new HashSet<long> { 1L };

        var flipped = (decimal)method!.Invoke(null, new object[] { 1L, 5m, usdQuoteIds })!;
        var same = (decimal)method!.Invoke(null, new object[] { 2L, 5m, usdQuoteIds })!;

        Assert.Equal(-5m, flipped);
        Assert.Equal(5m, same);
    }
}
