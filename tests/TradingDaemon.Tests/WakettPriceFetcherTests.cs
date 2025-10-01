using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using TradingDaemon.Data;
using TradingDaemon.Models;
using TradingDaemon.Services;
using Xunit;

public class WakettPriceFetcherTests
{
    [Fact]
    public void BuildComputedRates_ComputesCrossRates()
    {
        var baseSymbols = new List<WakettSecuritySymbol>
        {
            new() { SecurityId = 1, Symbol = "EUR/USD" },
            new() { SecurityId = 2, Symbol = "USD/JPY" }
        };
        var missingSymbols = new List<WakettSecuritySymbol>
        {
            new() { SecurityId = 3, Symbol = "EUR/JPY" }
        };
        var prices = new List<WakettPrice>
        {
            new() { Symbol = "EUR/USD", Mid = 1.2m },
            new() { Symbol = "USD/JPY", Mid = 110m }
        };

        var computed = WakettPriceFetcher.BuildComputedRates(
            baseSymbols,
            missingSymbols,
            prices,
            Mock.Of<ILogger<WakettPriceFetcher>>());

        var eurUsd = computed.Single(c => c.Definition.SecurityId == 1);
        var usdJpy = computed.Single(c => c.Definition.SecurityId == 2);
        var eurJpy = computed.Single(c => c.Definition.SecurityId == 3);

        Assert.Equal(1.2m, eurUsd.Rate);
        Assert.Equal(110m, usdJpy.Rate);
        Assert.Equal(132m, eurJpy.Rate);
    }

    [Fact]
    public void BuildComputedRates_UsesInverseRatesWhenNeeded()
    {
        var baseSymbols = new List<WakettSecuritySymbol>
        {
            new() { SecurityId = 1, Symbol = "USD/CAD" },
            new() { SecurityId = 2, Symbol = "NZD/USD" }
        };
        var missingSymbols = new List<WakettSecuritySymbol>
        {
            new() { SecurityId = 3, Symbol = "CAD/NZD" }
        };
        var prices = new List<WakettPrice>
        {
            new() { Symbol = "USD/CAD", Mid = 1.3m },
            new() { Symbol = "NZD/USD", Mid = 0.6m }
        };

        var computed = WakettPriceFetcher.BuildComputedRates(
            baseSymbols,
            missingSymbols,
            prices,
            Mock.Of<ILogger<WakettPriceFetcher>>());

        var cadNzd = computed.Single(c => c.Definition.SecurityId == 3);
        var expected = 50m / 39m;

        Assert.Equal(expected, cadNzd.Rate, 6);
    }

    [Theory]
    [InlineData("EUR/USD", "EUR", "USD")]
    [InlineData("USDJPY", "USD", "JPY")]
    [InlineData("GBP-USD", "GBP", "USD")]
    [InlineData("chfNzd", "CHF", "NZD")]
    public void TryParsePair_NormalizesInput(string symbol, string expectedBase, string expectedQuote)
    {
        Assert.True(WakettPriceFetcher.TryParsePair(symbol, out var pair));
        Assert.Equal(expectedBase, pair.Base);
        Assert.Equal(expectedQuote, pair.Quote);
    }

    [Fact]
    public void TryAdjustRateForSecurity_InvertsForOppositeOrientation()
    {
        var computedPair = new WakettPriceFetcher.CurrencyPair("EUR", "USD");
        var targetPair = new WakettPriceFetcher.CurrencyPair("USD", "EUR");

        var success = WakettPriceFetcher.TryAdjustRateForSecurity(1.25m, computedPair, targetPair, out var adjusted, out var inverted);

        Assert.True(success);
        Assert.True(inverted);
        Assert.Equal(0.8m, adjusted);
    }

    [Fact]
    public void TryComputeCrossRate_ReturnsFalseWhenDisconnected()
    {
        var graph = new Dictionary<string, Dictionary<string, decimal>>
        {
            ["EUR"] = new() { ["USD"] = 1.2m },
            ["USD"] = new() { ["EUR"] = 1m / 1.2m }
        };

        var success = WakettPriceFetcher.TryComputeCrossRate(
            graph,
            new WakettPriceFetcher.CurrencyPair("EUR", "JPY"),
            out var rate);

        Assert.False(success);
        Assert.Equal(0m, rate);
    }

    [Fact]
    public void DetermineTimestampUtc_PrefersResponseTimestamp()
    {
        var responseTs = "2024-03-01T12:30:00Z";
        var requestTs = new DateTimeOffset(2024, 3, 1, 12, 0, 0, TimeSpan.Zero);
        var fallback = new DateTime(2024, 3, 1, 10, 0, 0, DateTimeKind.Utc);

        var result = WakettPriceFetcher.DetermineTimestampUtc(responseTs, requestTs, fallback);

        Assert.Equal(new DateTime(2024, 3, 1, 12, 30, 0, DateTimeKind.Utc), result);
    }

    [Fact]
    public void DetermineTimestampUtc_PreservesResponseTimestampMinuteOffset()
    {
        var responseTs = "2024-03-01T12:06:00Z";
        var requestTs = new DateTimeOffset(2024, 3, 1, 11, 0, 0, TimeSpan.Zero);
        var fallback = new DateTime(2024, 3, 1, 10, 0, 0, DateTimeKind.Utc);

        var result = WakettPriceFetcher.DetermineTimestampUtc(responseTs, requestTs, fallback);

        Assert.Equal(new DateTime(2024, 3, 1, 12, 6, 0, DateTimeKind.Utc), result);
    }

    [Fact]
    public void DetermineTimestampUtc_FallsBackToRequest()
    {
        var requestTs = new DateTimeOffset(2024, 3, 1, 12, 6, 0, TimeSpan.Zero);
        var fallback = new DateTime(2024, 3, 1, 10, 0, 0, DateTimeKind.Utc);

        var result = WakettPriceFetcher.DetermineTimestampUtc(null, requestTs, fallback);

        Assert.Equal(new DateTime(2024, 3, 1, 12, 6, 0, DateTimeKind.Utc), result);
    }

    [Fact]
    public void DetermineTimestampUtc_UsesFallbackWhenNoTimestamp()
    {
        var fallback = new DateTime(2024, 3, 1, 12, 6, 0, DateTimeKind.Utc);

        var result = WakettPriceFetcher.DetermineTimestampUtc("not-a-date", null, fallback);

        Assert.Equal(fallback, result);
    }

    [Fact]
    public void BuildExpectedBarHours_SkipsWeekendHours()
    {
        var endHour = new DateTime(2024, 3, 4, 12, 0, 0, DateTimeKind.Utc); // Monday

        var hours = WakettPriceFetcher.BuildExpectedBarHours(endHour, 24);

        Assert.Equal(24, hours.Count);
        Assert.Equal(new DateTime(2024, 3, 1, 13, 0, 0, DateTimeKind.Utc), hours[0]);
        Assert.Equal(endHour, hours[^1]);
        Assert.DoesNotContain(hours, h => h.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday);
    }

    [Fact]
    public void PriceMinuteOffset_PrefersAutomationOptions()
    {
        var config = BuildConfiguration();
        var options = Options.Create(new WakettAutomationOptions { WorkflowMinuteOffset = 8 });

        var fetcher = CreateFetcher(config, options);
        var offset = GetPriceMinuteOffset(fetcher);

        Assert.Equal(8, offset);
    }

    [Fact]
    public void PriceMinuteOffset_FallsBackToConfigurationWhenAutomationOptionsMissing()
    {
        var config = BuildConfiguration();

        var fetcher = CreateFetcher(config, null);
        var offset = GetPriceMinuteOffset(fetcher);

        Assert.Equal(6, offset);
    }

    private static WakettPriceFetcher CreateFetcher(
        IConfiguration config,
        IOptions<WakettAutomationOptions>? automationOptions)
    {
        var httpClientFactory = Mock.Of<IHttpClientFactory>();
        var apiClient = new WakettApiClient(httpClientFactory);
        var context = new DapperContext(config);

        return new WakettPriceFetcher(
            apiClient,
            context,
            config,
            Mock.Of<ILogger<WakettPriceFetcher>>(),
            automationOptions);
    }

    private static IConfiguration BuildConfiguration()
    {
        var values = new Dictionary<string, string?>
        {
            ["ExternalApis:WakettApi:PriceMinuteOffset"] = "6",
            ["ConnectionStrings:DefaultConnection"] = "Server=.;Database=Test;Trusted_Connection=True;"
        };

        return new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();
    }

    private static int GetPriceMinuteOffset(WakettPriceFetcher fetcher)
    {
        var property = typeof(WakettPriceFetcher).GetProperty(
            "PriceMinuteOffset",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;

        return (int)property.GetValue(fetcher)!;
    }
}
