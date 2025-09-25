using System.Collections.Generic;
using System.Data;
using System.Net;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using Moq.Protected;
using TradingDaemon.Data;
using TradingDaemon.Services;
using Xunit;

public class OrderSenderTests
{
    [Fact]
    public async Task SendOrdersAsync_SubmitsLatestWeightsToWakett()
    {
        HttpRequestMessage? captured = null;
        var handler = new Mock<HttpMessageHandler>();
        handler.Protected().Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
            .Callback<HttpRequestMessage, CancellationToken>((message, _) => captured = message)
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{\"ts\":\"\",\"orders\":[]}")
            });

        var client = new HttpClient(handler.Object) { BaseAddress = new Uri("http://wakett") };
        var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient("WakettApi") == client);
        var apiClient = new WakettApiClient(factory);

        var now = new DateTimeOffset(2024, 1, 2, 15, 0, 0, TimeSpan.Zero);
        var barTimeUtc = now.AddMinutes(-30).UtcDateTime;
        var weights = new List<OrderSender.TheoreticalWeightRow>
        {
            new() { SecurityId = 58, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 10, Weight = 0.15m },
            new() { SecurityId = 61, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 10, Weight = -0.05m }
        };

        var configuration = BuildConfiguration(new Dictionary<string, string?>
        {
            ["ExternalApis:WakettApi:Aum"] = "2500000"
        });

        var symbolMap = new Dictionary<int, string>
        {
            [58] = "EURUSD",
            [61] = "EURCHF",
            [136] = "USDCHF"
        };

        var context = new Mock<DapperContext>(configuration);
        context.Setup(c => c.CreateConnection()).Returns(Mock.Of<IDbConnection>());

        var logger = Mock.Of<ILogger<OrderSender>>();
        var timeProvider = new TestTimeProvider(now);

        var sender = new TestOrderSender(apiClient, context.Object, logger, configuration, timeProvider, weights, symbolMap);

        await sender.SendOrdersAsync();

        handler.Protected().Verify(
            "SendAsync",
            Times.Once(),
            ItExpr.IsAny<HttpRequestMessage>(),
            ItExpr.IsAny<CancellationToken>());

        Assert.NotNull(captured);
        var payload = await captured!.Content.ReadAsStringAsync();
        using var document = JsonDocument.Parse(payload);
        var root = document.RootElement;

        Assert.Equal(OrderSender.FormatTimestamp(barTimeUtc), root.GetProperty("ts").GetString());
        Assert.Equal(2_500_000d, root.GetProperty("aum").GetDouble());

        var orders = root.GetProperty("orders");
        Assert.Equal(2, orders.GetArrayLength());

        var first = orders[0];
        Assert.Equal("EURUSD", first.GetProperty("symbol").GetString());
        Assert.Equal("BUY", first.GetProperty("side").GetString());
        Assert.Equal("QQB-58", first.GetProperty("code").GetString());
        Assert.Equal("percentage", first.GetProperty("size").GetProperty("type").GetString());
        Assert.Equal(0.1, first.GetProperty("size").GetProperty("value").GetDouble(), 6);

        var second = orders[1];
        Assert.Equal("USDCHF", second.GetProperty("symbol").GetString());
        Assert.Equal("SELL", second.GetProperty("side").GetString());
        Assert.Equal("QQB-136", second.GetProperty("code").GetString());
        Assert.Equal(0.05, second.GetProperty("size").GetProperty("value").GetDouble(), 6);
    }

    [Fact]
    public async Task SendOrdersAsync_NetsCrossesIntoUsdPairs()
    {
        HttpRequestMessage? captured = null;
        var handler = new Mock<HttpMessageHandler>();
        handler.Protected().Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
            .Callback<HttpRequestMessage, CancellationToken>((message, _) => captured = message)
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{\"ts\":\"\",\"orders\":[]}")
            });

        var client = new HttpClient(handler.Object) { BaseAddress = new Uri("http://wakett") };
        var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient("WakettApi") == client);
        var apiClient = new WakettApiClient(factory);

        var now = new DateTimeOffset(2024, 1, 2, 15, 0, 0, TimeSpan.Zero);
        var barTimeUtc = now.AddMinutes(-30).UtcDateTime;
        var weights = new List<OrderSender.TheoreticalWeightRow>
        {
            new() { SecurityId = 61, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 11, Weight = 0.2m },
            new() { SecurityId = 62, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 11, Weight = 0.1m },
            new() { SecurityId = 65, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 11, Weight = -0.1m }
        };

        var configuration = BuildConfiguration(new Dictionary<string, string?>());

        var symbolMap = new Dictionary<int, string>
        {
            [61] = "EURCHF",
            [62] = "CADJPY",
            [65] = "USDJPY",
            [66] = "EURUSD",
            [64] = "USDCAD",
            [136] = "USDCHF"
        };

        var context = new Mock<DapperContext>(configuration);
        context.Setup(c => c.CreateConnection()).Returns(Mock.Of<IDbConnection>());

        var logger = Mock.Of<ILogger<OrderSender>>();
        var timeProvider = new TestTimeProvider(now);

        var sender = new TestOrderSender(apiClient, context.Object, logger, configuration, timeProvider, weights, symbolMap);

        await sender.SendOrdersAsync();

        handler.Protected().Verify(
            "SendAsync",
            Times.Once(),
            ItExpr.IsAny<HttpRequestMessage>(),
            ItExpr.IsAny<CancellationToken>());

        Assert.NotNull(captured);
        var payload = await captured!.Content.ReadAsStringAsync();
        using var document = JsonDocument.Parse(payload);
        var orders = document.RootElement.GetProperty("orders");

        Assert.Equal(3, orders.GetArrayLength());

        var first = orders[0];
        Assert.Equal("USDCAD", first.GetProperty("symbol").GetString());
        Assert.Equal("SELL", first.GetProperty("side").GetString());
        Assert.Equal("QQB-64", first.GetProperty("code").GetString());
        Assert.Equal(0.1, first.GetProperty("size").GetProperty("value").GetDouble(), 6);

        var second = orders[1];
        Assert.Equal("EURUSD", second.GetProperty("symbol").GetString());
        Assert.Equal("BUY", second.GetProperty("side").GetString());
        Assert.Equal("QQB-66", second.GetProperty("code").GetString());
        Assert.Equal(0.2, second.GetProperty("size").GetProperty("value").GetDouble(), 6);

        var third = orders[2];
        Assert.Equal("USDCHF", third.GetProperty("symbol").GetString());
        Assert.Equal("BUY", third.GetProperty("side").GetString());
        Assert.Equal("QQB-136", third.GetProperty("code").GetString());
        Assert.Equal(0.2, third.GetProperty("size").GetProperty("value").GetDouble(), 6);
    }

    [Fact]
    public async Task SendOrdersAsync_DoesNotSendWhenWeightsAreStale()
    {
        var handler = new Mock<HttpMessageHandler>();
        handler.Protected().Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK));

        var client = new HttpClient(handler.Object) { BaseAddress = new Uri("http://wakett") };
        var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient("WakettApi") == client);
        var apiClient = new WakettApiClient(factory);

        var configuration = BuildConfiguration(new Dictionary<string, string?>());

        var symbolMap = new Dictionary<int, string>
        {
            [58] = "EURUSD"
        };

        var context = new Mock<DapperContext>(configuration);
        context.Setup(c => c.CreateConnection()).Returns(Mock.Of<IDbConnection>());

        var logger = Mock.Of<ILogger<OrderSender>>();

        var zone = TimeZoneInfo.FindSystemTimeZoneById(RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Central European Standard Time" : "Europe/Berlin");
        var nowLocal = new DateTime(2024, 1, 2, 12, 0, 0);
        var nowUtc = TimeZoneInfo.ConvertTimeToUtc(nowLocal, zone);
        var staleLocal = nowLocal.AddHours(-3);
        var staleUtc = TimeZoneInfo.ConvertTimeToUtc(staleLocal, zone);

        var timeProvider = new TestTimeProvider(new DateTimeOffset(nowUtc, TimeSpan.Zero));

        var weights = new List<OrderSender.TheoreticalWeightRow>
        {
            new() { SecurityId = 58, ModelId = 1, BarTimeUtc = staleUtc, ModelRunId = 1, Weight = 0.2m }
        };

        var sender = new TestOrderSender(apiClient, context.Object, logger, configuration, timeProvider, weights, symbolMap);

        await sender.SendOrdersAsync();

        handler.Protected().Verify(
            "SendAsync",
            Times.Never(),
            ItExpr.IsAny<HttpRequestMessage>(),
            ItExpr.IsAny<CancellationToken>());
    }

    [Fact]
    public async Task SendOrdersAsync_UsesPreviousDayWeightsForFirstTrade()
    {
        HttpRequestMessage? captured = null;
        var handler = new Mock<HttpMessageHandler>();
        handler.Protected().Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
            .Callback<HttpRequestMessage, CancellationToken>((message, _) => captured = message)
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{\"ts\":\"\",\"orders\":[]}")
            });

        var client = new HttpClient(handler.Object) { BaseAddress = new Uri("http://wakett") };
        var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient("WakettApi") == client);
        var apiClient = new WakettApiClient(factory);

        var configuration = BuildConfiguration(new Dictionary<string, string?>());

        var symbolMap = new Dictionary<int, string>
        {
            [58] = "EURUSD"
        };

        var context = new Mock<DapperContext>(configuration);
        context.Setup(c => c.CreateConnection()).Returns(Mock.Of<IDbConnection>());

        var logger = Mock.Of<ILogger<OrderSender>>();

        var zoneId = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Central European Standard Time" : "Europe/Berlin";
        var zone = TimeZoneInfo.FindSystemTimeZoneById(zoneId);

        var nowLocal = new DateTime(2024, 1, 2, 7, 0, 0);
        var nowUtc = TimeZoneInfo.ConvertTimeToUtc(nowLocal, zone);
        var previousDayLocal = new DateTime(2024, 1, 1, 15, 59, 0);
        var previousDayUtc = TimeZoneInfo.ConvertTimeToUtc(previousDayLocal, zone);

        var timeProvider = new TestTimeProvider(new DateTimeOffset(nowUtc, TimeSpan.Zero));

        var weights = new List<OrderSender.TheoreticalWeightRow>
        {
            new() { SecurityId = 58, ModelId = 1, BarTimeUtc = previousDayUtc, ModelRunId = 1, Weight = 0.3m }
        };

        var sender = new TestOrderSender(apiClient, context.Object, logger, configuration, timeProvider, weights, symbolMap);

        await sender.SendOrdersAsync();

        handler.Protected().Verify(
            "SendAsync",
            Times.Once(),
            ItExpr.IsAny<HttpRequestMessage>(),
            ItExpr.IsAny<CancellationToken>());

        Assert.NotNull(captured);
        var payload = await captured!.Content.ReadAsStringAsync();
        using var document = JsonDocument.Parse(payload);
        var root = document.RootElement;
        Assert.Equal(OrderSender.FormatTimestamp(previousDayUtc), root.GetProperty("ts").GetString());
    }

    private static IConfigurationRoot BuildConfiguration(IDictionary<string, string?> values)
        => new ConfigurationBuilder().AddInMemoryCollection(values).Build();

    private sealed class TestOrderSender : OrderSender
    {
        private readonly IReadOnlyList<TheoreticalWeightRow> _weights;
        private readonly IReadOnlyDictionary<int, string> _symbolMap;

        public TestOrderSender(
            WakettApiClient client,
            DapperContext context,
            ILogger<OrderSender> logger,
            IConfiguration configuration,
            TimeProvider timeProvider,
            IReadOnlyList<TheoreticalWeightRow> weights,
            IReadOnlyDictionary<int, string> symbolMap)
            : base(client, context, logger, configuration, timeProvider)
        {
            _weights = weights;
            _symbolMap = symbolMap;
        }

        protected override Task<IReadOnlyList<TheoreticalWeightRow>> LoadLatestWeightsAsync(
            IDbConnection connection,
            CancellationToken cancellationToken)
            => Task.FromResult(_weights);

        protected override Task<Dictionary<int, string>> LoadSymbolMapAsync(
            IDbConnection connection,
            CancellationToken cancellationToken)
            => Task.FromResult(new Dictionary<int, string>(_symbolMap));
    }

    private sealed class TestTimeProvider : TimeProvider
    {
        private readonly DateTimeOffset _utcNow;

        public TestTimeProvider(DateTimeOffset utcNow)
        {
            _utcNow = utcNow;
        }

        public override DateTimeOffset GetUtcNow() => _utcNow;
    }
}
