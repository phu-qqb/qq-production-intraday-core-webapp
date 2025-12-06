using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Moq.Protected;
using TradingDaemon.Data;
using TradingDaemon.Models;
using TradingDaemon.Options;
using TradingDaemon.Services;
using Xunit;

public class OrderSenderTests
{
    private static readonly IDatabaseObjectNameProvider DefaultDatabaseNameProvider = new DatabaseObjectNameProvider();
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

        var now = new DateTimeOffset(2024, 1, 2, 16, 30, 0, TimeSpan.Zero);
        var barTimeUtc = now.AddMinutes(-60).UtcDateTime;
        var weights = new List<OrderSender.NettedWeightRow>
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

        var sender = new TestOrderSender(
            apiClient,
            context.Object,
            logger,
            configuration,
            timeProvider,
            weights,
            symbolMap,
            new OrderSender.ModelScheduleRow { Offset = 60, BarSize = 0 });

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

        var expectedOrderTimestampUtc = OrderSender.CalculateOrderTimestamp(now.UtcDateTime, "US");
        Assert.Equal(OrderSender.FormatTimestamp(expectedOrderTimestampUtc), root.GetProperty("ts").GetString());
        Assert.Equal(2_500_000d, root.GetProperty("aum").GetDouble());
        Assert.Equal("EOC", root.GetProperty("execution").GetString());

        var orders = root.GetProperty("orders");
        Assert.Equal(2, orders.GetArrayLength());

        var first = orders[0];
        Assert.Equal("EUR/USD", first.GetProperty("symbol").GetString());
        Assert.Equal("BUY", first.GetProperty("side").GetString());
        Assert.Equal(OrderSender.BuildOrderCode(58, expectedOrderTimestampUtc), first.GetProperty("code").GetString());
        Assert.Equal("percentage", first.GetProperty("size").GetProperty("type").GetString());
        Assert.Equal(0.1, first.GetProperty("size").GetProperty("value").GetDouble(), 6);

        var second = orders[1];
        Assert.Equal("USD/CHF", second.GetProperty("symbol").GetString());
        Assert.Equal("SELL", second.GetProperty("side").GetString());
        Assert.Equal(OrderSender.BuildOrderCode(136, expectedOrderTimestampUtc), second.GetProperty("code").GetString());
        Assert.Equal(0.05, second.GetProperty("size").GetProperty("value").GetDouble(), 6);
    }

    [Fact]
    public async Task SendOrdersAsync_SubmitsFlatOrderWhenAllWeightsZero()
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

        var now = new DateTimeOffset(2024, 1, 2, 16, 30, 0, TimeSpan.Zero);
        var barTimeUtc = now.AddMinutes(-60).UtcDateTime;
        var weights = new List<OrderSender.NettedWeightRow>
        {
            new() { SecurityId = 58, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 10, Weight = 0m },
            new() { SecurityId = 61, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 10, Weight = 0m }
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

        var sender = new TestOrderSender(
            apiClient,
            context.Object,
            logger,
            configuration,
            timeProvider,
            weights,
            symbolMap,
            new OrderSender.ModelScheduleRow { Offset = 60, BarSize = 0 });

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

        var expectedOrderTimestampUtc = OrderSender.CalculateOrderTimestamp(now.UtcDateTime, "US");

        Assert.True(root.TryGetProperty("execution", out var execution));
        Assert.Equal("EOF", execution.GetString());

        var orders = root.GetProperty("orders");
        Assert.Equal(2, orders.GetArrayLength());

        var first = orders[0];
        Assert.Equal("EUR/USD", first.GetProperty("symbol").GetString());
        Assert.Equal("BUY", first.GetProperty("side").GetString());
        Assert.Equal(OrderSender.BuildOrderCode(58, expectedOrderTimestampUtc), first.GetProperty("code").GetString());
        Assert.Equal("percentage", first.GetProperty("size").GetProperty("type").GetString());
        Assert.Equal(0d, first.GetProperty("size").GetProperty("value").GetDouble());

        var second = orders[1];
        Assert.Equal("USD/CHF", second.GetProperty("symbol").GetString());
        Assert.Equal("BUY", second.GetProperty("side").GetString());
        Assert.Equal(OrderSender.BuildOrderCode(136, expectedOrderTimestampUtc), second.GetProperty("code").GetString());
        Assert.Equal("percentage", second.GetProperty("size").GetProperty("type").GetString());
        Assert.Equal(0d, second.GetProperty("size").GetProperty("value").GetDouble());
    }

    [Fact]
    public async Task SendOrdersAsync_SkipsAfterUsHolidayCutoff()
    {
        var handler = new Mock<HttpMessageHandler>();
        handler.Protected().Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{\"ts\":\"\",\"orders\":[]}")
            });

        var client = new HttpClient(handler.Object) { BaseAddress = new Uri("http://wakett") };
        var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient("WakettApi") == client);
        var apiClient = new WakettApiClient(factory);

        var now = new DateTimeOffset(2024, 7, 4, 19, 30, 0, TimeSpan.Zero);
        var barTimeUtc = now.AddMinutes(-60).UtcDateTime;
        var weights = new List<OrderSender.NettedWeightRow>
        {
            new() { SecurityId = 58, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 10, Weight = 0.15m }
        };

        var configuration = BuildConfiguration(new Dictionary<string, string?>
        {
            ["ExternalApis:WakettApi:Aum"] = "2500000",
            ["Trading:UsBankHoliday"] = "true"
        });

        var symbolMap = new Dictionary<int, string>
        {
            [58] = "EURUSD"
        };

        var context = new Mock<DapperContext>(configuration);
        context.Setup(c => c.CreateConnection()).Returns(Mock.Of<IDbConnection>());

        var logger = Mock.Of<ILogger<OrderSender>>();
        var timeProvider = new TestTimeProvider(now);

        var sender = new TestOrderSender(
            apiClient,
            context.Object,
            logger,
            configuration,
            timeProvider,
            weights,
            symbolMap,
            new OrderSender.ModelScheduleRow { Offset = 60, BarSize = 0 },
            tradingOptions: new TradingOptions { UsBankHoliday = true });

        await sender.SendOrdersAsync();

        handler.Protected().Verify(
            "SendAsync",
            Times.Never(),
            ItExpr.IsAny<HttpRequestMessage>(),
            ItExpr.IsAny<CancellationToken>());
    }

    [Fact]
    public async Task SendOrdersAsync_SubmitsFlatEofOrderOnUsBankHoliday()
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

        var now = new DateTimeOffset(2024, 7, 4, 16, 40, 0, TimeSpan.Zero);
        var barTimeUtc = now.AddMinutes(-60).UtcDateTime;
        var weights = new List<OrderSender.NettedWeightRow>
        {
            new() { SecurityId = 58, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 10, Weight = 0.25m },
            new() { SecurityId = 136, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 10, Weight = -0.1m }
        };

        var configuration = BuildConfiguration(new Dictionary<string, string?>
        {
            ["ExternalApis:WakettApi:Aum"] = "2500000",
            ["Trading:UsBankHoliday"] = "true"
        });

        var symbolMap = new Dictionary<int, string>
        {
            [58] = "EURUSD",
            [136] = "USDCHF"
        };

        var context = new Mock<DapperContext>(configuration);
        context.Setup(c => c.CreateConnection()).Returns(Mock.Of<IDbConnection>());

        var logger = Mock.Of<ILogger<OrderSender>>();
        var timeProvider = new TestTimeProvider(now);

        var sender = new TestOrderSender(
            apiClient,
            context.Object,
            logger,
            configuration,
            timeProvider,
            weights,
            symbolMap,
            new OrderSender.ModelScheduleRow { Offset = 60, BarSize = 0 },
            tradingOptions: new TradingOptions { UsBankHoliday = true });

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

        Assert.True(root.TryGetProperty("execution", out var execution));
        Assert.Equal("EOF", execution.GetString());

        var orders = root.GetProperty("orders");
        Assert.Equal(2, orders.GetArrayLength());

        var expectedOrderTimestampUtc = OrderSender.CalculateOrderTimestamp(now.UtcDateTime, "US", true);

        var first = orders[0];
        Assert.Equal("EUR/USD", first.GetProperty("symbol").GetString());
        Assert.Equal("BUY", first.GetProperty("side").GetString());
        Assert.Equal(OrderSender.BuildOrderCode(58, expectedOrderTimestampUtc), first.GetProperty("code").GetString());
        Assert.Equal("percentage", first.GetProperty("size").GetProperty("type").GetString());
        Assert.Equal(0d, first.GetProperty("size").GetProperty("value").GetDouble());

        var second = orders[1];
        Assert.Equal("USD/CHF", second.GetProperty("symbol").GetString());
        Assert.Equal("BUY", second.GetProperty("side").GetString());
        Assert.Equal(OrderSender.BuildOrderCode(136, expectedOrderTimestampUtc), second.GetProperty("code").GetString());
        Assert.Equal("percentage", second.GetProperty("size").GetProperty("type").GetString());
        Assert.Equal(0d, second.GetProperty("size").GetProperty("value").GetDouble());
    }

    [Fact]
    public async Task SendOrdersAsync_SendsIntradayOrdersDuringUsBankHoliday()
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

        var now = new DateTimeOffset(2024, 7, 4, 15, 50, 0, TimeSpan.Zero);
        var barTimeUtc = now.AddMinutes(-60).UtcDateTime;
        var weights = new List<OrderSender.NettedWeightRow>
        {
            new() { SecurityId = 58, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 10, Weight = 0.2m },
            new() { SecurityId = 136, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 10, Weight = -0.1m }
        };

        var configuration = BuildConfiguration(new Dictionary<string, string?>
        {
            ["ExternalApis:WakettApi:Aum"] = "2500000",
            ["Trading:UsBankHoliday"] = "true"
        });

        var symbolMap = new Dictionary<int, string>
        {
            [58] = "EURUSD",
            [136] = "USDCHF"
        };

        var context = new Mock<DapperContext>(configuration);
        context.Setup(c => c.CreateConnection()).Returns(Mock.Of<IDbConnection>());

        var logger = Mock.Of<ILogger<OrderSender>>();
        var timeProvider = new TestTimeProvider(now);

        var sender = new TestOrderSender(
            apiClient,
            context.Object,
            logger,
            configuration,
            timeProvider,
            weights,
            symbolMap,
            new OrderSender.ModelScheduleRow { Offset = 60, BarSize = 0 },
            tradingOptions: new TradingOptions { UsBankHoliday = true });

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

        Assert.Equal("EOC", root.GetProperty("execution").GetString());

        var orders = root.GetProperty("orders");
        Assert.Equal(2, orders.GetArrayLength());

        var expectedOrderTimestampUtc = OrderSender.CalculateOrderTimestamp(now.UtcDateTime, "US", true);

        var first = orders[0];
        Assert.Equal("EUR/USD", first.GetProperty("symbol").GetString());
        Assert.Equal("BUY", first.GetProperty("side").GetString());
        Assert.Equal(OrderSender.BuildOrderCode(58, expectedOrderTimestampUtc), first.GetProperty("code").GetString());
        Assert.Equal("percentage", first.GetProperty("size").GetProperty("type").GetString());
        Assert.Equal(0.2, first.GetProperty("size").GetProperty("value").GetDouble(), 6);

        var second = orders[1];
        Assert.Equal("USD/CHF", second.GetProperty("symbol").GetString());
        Assert.Equal("SELL", second.GetProperty("side").GetString());
        Assert.Equal(OrderSender.BuildOrderCode(136, expectedOrderTimestampUtc), second.GetProperty("code").GetString());
        Assert.Equal("percentage", second.GetProperty("size").GetProperty("type").GetString());
        Assert.Equal(0.1, second.GetProperty("size").GetProperty("value").GetDouble(), 6);
    }

    [Theory]
    [InlineData(2024, 1, 1, 17, 0, 0, false)] // 06:00 NZDT
    [InlineData(2024, 1, 1, 18, 0, 0, true)]  // 07:00 NZDT
    [InlineData(2024, 6, 30, 18, 55, 0, false)] // 06:55 NZST
    [InlineData(2024, 6, 30, 19, 0, 0, true)]  // 07:00 NZST
    public void ApplyWeightOverrides_UsesNewZealandLocalTime(
        int year,
        int month,
        int day,
        int hour,
        int minute,
        int second,
        bool expectZero)
    {
        var orderTimestampUtc = new DateTime(year, month, day, hour, minute, second, DateTimeKind.Utc);
        var barTimeUtc = orderTimestampUtc.AddMinutes(-60);

        var weights = new List<OrderSender.NettedWeightRow>
        {
            new() { SecurityId = 1, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 10, Weight = 0.12m },
            new() { SecurityId = 2, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 10, Weight = -0.03m }
        };

        var symbolMap = new Dictionary<int, string>
        {
            [1] = "NZDUSD",
            [2] = "EURUSD"
        };

        var configuration = BuildConfiguration(new Dictionary<string, string?>());
        var sender = CreateSessionResolvingSender(configuration);

        var result = sender.InvokeApplyWeightOverrides(weights, symbolMap, orderTimestampUtc);

        var nzdusd = Assert.Single(result, row => row.SecurityId == 1);
        var eurusd = Assert.Single(result, row => row.SecurityId == 2);

        Assert.Equal(expectZero ? 0m : 0.12m, nzdusd.Weight);
    //    Assert.Equal(-0.03m, eurusd.Weight);
    //}

    //[Fact]
    //public async Task SendOrdersAsync_DoesNotSendWhenOrdersAlreadyExist()
    //{
    //    var handler = new Mock<HttpMessageHandler>(MockBehavior.Strict);
    //    var client = new HttpClient(handler.Object) { BaseAddress = new Uri("http://wakett") };
    //    var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient("WakettApi") == client);
    //    var apiClient = new WakettApiClient(factory);

    //    var now = new DateTimeOffset(2024, 1, 2, 16, 30, 0, TimeSpan.Zero);
    //    var barTimeUtc = now.AddMinutes(-60).UtcDateTime;
    //    var weights = new List<OrderSender.NettedWeightRow>
    //    {
    //        new() { SecurityId = 58, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 10, Weight = 0.15m },
    //        new() { SecurityId = 61, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 10, Weight = -0.05m }
    //    };

    //    var configuration = BuildConfiguration(new Dictionary<string, string?>
    //    {
    //        ["ExternalApis:WakettApi:Aum"] = "2500000"
    //    });

    //    var symbolMap = new Dictionary<int, string>
    //    {
    //        [58] = "EURUSD",
    //        [61] = "EURCHF",
    //        [136] = "USDCHF"
    //    };

    //    var context = new Mock<DapperContext>(configuration);
    //    context.Setup(c => c.CreateConnection()).Returns(Mock.Of<IDbConnection>());

    //    var logger = Mock.Of<ILogger<OrderSender>>();
    //    var timeProvider = new TestTimeProvider(now);

    //    var sender = new TestOrderSender(
    //        apiClient,
    //        context.Object,
    //        logger,
    //        configuration,
    //        timeProvider,
    //        weights,
    //        symbolMap,
    //        new OrderSender.ModelScheduleRow { Offset = 60, BarSize = 0 },
    //        existingOrderSymbols: new[] { "EUR/USD" });

    //    await sender.SendOrdersAsync();

    //    handler.Protected().Verify(
    //        "SendAsync",
    //        Times.Never(),
    //        ItExpr.IsAny<HttpRequestMessage>(),
    //        ItExpr.IsAny<CancellationToken>());
    //}

    //[Fact]
    //public async Task SendOrdersAsync_DoesNotSendWhenTradingLimitBreached()
    //{
    //    var handler = new Mock<HttpMessageHandler>(MockBehavior.Strict);
    //    var client = new HttpClient(handler.Object) { BaseAddress = new Uri("http://wakett") };
    //    var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient("WakettApi") == client);
    //    var apiClient = new WakettApiClient(factory);

    //    var now = new DateTimeOffset(2024, 1, 2, 16, 30, 0, TimeSpan.Zero);
    //    var barTimeUtc = now.AddMinutes(-60).UtcDateTime;
    //    var weights = new List<OrderSender.NettedWeightRow>
    //    {
    //        new() { SecurityId = 58, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 10, Weight = 0.15m },
    //        new() { SecurityId = 61, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 10, Weight = -0.05m }
    //    };

    //    var configuration = BuildConfiguration(new Dictionary<string, string?>
    //    {
    //        ["ExternalApis:WakettApi:Aum"] = "2500000"
    //    });

    //    var symbolMap = new Dictionary<int, string>
    //    {
    //        [58] = "EURUSD",
    //        [61] = "EURCHF",
    //        [136] = "USDCHF"
    //    };

    //    var context = new Mock<DapperContext>(configuration);
    //    context.Setup(c => c.CreateConnection()).Returns(Mock.Of<IDbConnection>());

    //    var logger = Mock.Of<ILogger<OrderSender>>();
    //    var timeProvider = new TestTimeProvider(now);

    //    var tradingLimit = TestOrderSender.CreateTradingLimit(singleTradeGross: 0.05m);

    //    var sender = new TestOrderSender(
    //        apiClient,
    //        context.Object,
    //        logger,
    //        configuration,
    //        timeProvider,
    //        weights,
    //        symbolMap,
    //        new OrderSender.ModelScheduleRow { Offset = 60, BarSize = 0 },
    //        tradingLimit);

    //    await sender.SendOrdersAsync();

    //    handler.Protected().Verify(
    //        "SendAsync",
    //        Times.Never(),
    //        ItExpr.IsAny<HttpRequestMessage>(),
    //        ItExpr.IsAny<CancellationToken>());

    //    Assert.Single(sender.LoggedBreaches);
    //    var breach = sender.LoggedBreaches[0];
    //    Assert.Equal("SingleTradeGrossLimit", breach.LimitType);
    //}

    //[Fact]
    //public async Task SendOrdersAsync_UsesWeightScaleForTurnoverLimits()
    //{
    //    var handler = new Mock<HttpMessageHandler>();
    //    handler.Protected().Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
    //        .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
    //        {
    //            Content = new StringContent("{\"ts\":\"\",\"orders\":[]}")
    //        });

    //    var client = new HttpClient(handler.Object) { BaseAddress = new Uri("http://wakett") };
    //    var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient("WakettApi") == client);
    //    var apiClient = new WakettApiClient(factory);

    //    var now = new DateTimeOffset(2024, 1, 2, 16, 30, 0, TimeSpan.Zero);
    //    var barTimeUtc = now.AddMinutes(-60).UtcDateTime;
    //    var weights = new List<OrderSender.NettedWeightRow>
    //    {
    //        new() { SecurityId = 58, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 10, Weight = 0.15m }
    //    };

    //    var configuration = BuildConfiguration(new Dictionary<string, string?>
    //    {
    //        ["ExternalApis:WakettApi:Aum"] = "2500000"
    //    });

    //    var symbolMap = new Dictionary<int, string>
    //    {
    //        [58] = "EURUSD"
    //    };

    //    var context = new Mock<DapperContext>(configuration);
    //    context.Setup(c => c.CreateConnection()).Returns(Mock.Of<IDbConnection>());

    //    var logger = Mock.Of<ILogger<OrderSender>>();
    //    var timeProvider = new TestTimeProvider(now);

    //    var tradingLimit = TestOrderSender.CreateTradingLimit(
    //        singleTradeTurnover: 0.10m,
    //        totalTurnover: 0.12m);

    //    var sender = new TestOrderSender(
    //        apiClient,
    //        context.Object,
    //        logger,
    //        configuration,
    //        timeProvider,
    //        weights,
    //        symbolMap,
    //        new OrderSender.ModelScheduleRow { Offset = 60, BarSize = 0 },
    //        tradingLimit);

    //    await sender.SendOrdersAsync();

    //    handler.Protected().Verify(
    //        "SendAsync",
    //        Times.Never(),
    //        ItExpr.IsAny<HttpRequestMessage>(),
    //        ItExpr.IsAny<CancellationToken>());

    //    Assert.Equal(2, sender.LoggedBreaches.Count);

    //    var single = sender.LoggedBreaches[0];
    //    Assert.Equal("SingleTradeTurnoverLimit", single.LimitType);
    //    Assert.Equal(0.10m, single.LimitValue);
    //    Assert.Equal(0.15m, single.ObservedValue);
    //    Assert.Contains("weight", single.Message, StringComparison.OrdinalIgnoreCase);
    //    Assert.Contains("Equivalent notional", single.Message, StringComparison.OrdinalIgnoreCase);

    //    var total = sender.LoggedBreaches[1];
    //    Assert.Equal("TotalTurnoverLimit", total.LimitType);
    //    Assert.Equal(0.12m, total.LimitValue);
    //    Assert.Equal(0.15m, total.ObservedValue);
    //    Assert.Contains("weight", total.Message, StringComparison.OrdinalIgnoreCase);
    //    Assert.Contains("Equivalent notional", total.Message, StringComparison.OrdinalIgnoreCase);
    //}

    //[Fact]
    //public async Task SendOrdersAsync_UsesOverrideAumWhenProvided()
    //{
    //    HttpRequestMessage? captured = null;
    //    var handler = new Mock<HttpMessageHandler>();
    //    handler.Protected().Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
    //        .Callback<HttpRequestMessage, CancellationToken>((message, _) => captured = message)
    //        .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
    //        {
    //            Content = new StringContent("{\"ts\":\"\",\"orders\":[]}")
    //        });

    //    var client = new HttpClient(handler.Object) { BaseAddress = new Uri("http://wakett") };
    //    var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient("WakettApi") == client);
    //    var apiClient = new WakettApiClient(factory);

    //    var now = new DateTimeOffset(2024, 1, 2, 16, 30, 0, TimeSpan.Zero);
    //    var barTimeUtc = now.AddMinutes(-60).UtcDateTime;
    //    var weights = new List<OrderSender.NettedWeightRow>
    //    {
    //        new() { SecurityId = 58, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 10, Weight = 0.15m }
    //    };

    //    var configuration = BuildConfiguration(new Dictionary<string, string?>
    //    {
    //        ["ExternalApis:WakettApi:Aum"] = "2500000"
    //    });

    //    var symbolMap = new Dictionary<int, string>
    //    {
    //        [58] = "EURUSD"
    //    };

    //    var context = new Mock<DapperContext>(configuration);
    //    context.Setup(c => c.CreateConnection()).Returns(Mock.Of<IDbConnection>());

    //    var logger = Mock.Of<ILogger<OrderSender>>();
    //    var timeProvider = new TestTimeProvider(now);

    //    var sender = new TestOrderSender(
    //        apiClient,
    //        context.Object,
    //        logger,
    //        configuration,
    //        timeProvider,
    //        weights,
    //        symbolMap,
    //        new OrderSender.ModelScheduleRow { Offset = 60, BarSize = 0 });

    //    await sender.SendOrdersAsync(3_100_000d);

    //    Assert.NotNull(captured);
    //    var payload = await captured!.Content.ReadAsStringAsync();
    //    using var document = JsonDocument.Parse(payload);
    //    var root = document.RootElement;
    //    Assert.Equal(3_100_000d, root.GetProperty("aum").GetDouble());
    //}

    //[Fact]
    //public async Task SendOrdersAsync_NetsCrossesIntoUsdPairs()
    //{
    //    HttpRequestMessage? captured = null;
    //    var handler = new Mock<HttpMessageHandler>();
    //    handler.Protected().Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
    //        .Callback<HttpRequestMessage, CancellationToken>((message, _) => captured = message)
    //        .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
    //        {
    //            Content = new StringContent("{\"ts\":\"\",\"orders\":[]}")
    //        });

    //    var client = new HttpClient(handler.Object) { BaseAddress = new Uri("http://wakett") };
    //    var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient("WakettApi") == client);
    //    var apiClient = new WakettApiClient(factory);

    //    var now = new DateTimeOffset(2024, 1, 2, 16, 30, 0, TimeSpan.Zero);
    //    var barTimeUtc = now.AddMinutes(-60).UtcDateTime;
    //    var weights = new List<OrderSender.NettedWeightRow>
    //    {
    //        new() { SecurityId = 61, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 11, Weight = 0.2m },
    //        new() { SecurityId = 62, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 11, Weight = 0.1m },
    //        new() { SecurityId = 65, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 11, Weight = -0.1m }
    //    };

    //    var configuration = BuildConfiguration(new Dictionary<string, string?>());

    //    var symbolMap = new Dictionary<int, string>
    //    {
    //        [61] = "EURCHF",
    //        [62] = "CADJPY",
    //        [65] = "USDJPY",
    //        [66] = "EURUSD",
    //        [64] = "USDCAD",
    //        [136] = "USDCHF"
    //    };

    //    var context = new Mock<DapperContext>(configuration);
    //    context.Setup(c => c.CreateConnection()).Returns(Mock.Of<IDbConnection>());

    //    var logger = Mock.Of<ILogger<OrderSender>>();
    //    var timeProvider = new TestTimeProvider(now);

    //    var sender = new TestOrderSender(
    //        apiClient,
    //        context.Object,
    //        logger,
    //        configuration,
    //        timeProvider,
    //        weights,
    //        symbolMap,
    //        new OrderSender.ModelScheduleRow { Offset = 60, BarSize = 0 });

    //    await sender.SendOrdersAsync();

    //    handler.Protected().Verify(
    //        "SendAsync",
    //        Times.Once(),
    //        ItExpr.IsAny<HttpRequestMessage>(),
    //        ItExpr.IsAny<CancellationToken>());

    //    Assert.NotNull(captured);
    //    var payload = await captured!.Content.ReadAsStringAsync();
    //    using var document = JsonDocument.Parse(payload);
    //    var orders = document.RootElement.GetProperty("orders");

    //    var expectedOrderTimestampUtc = OrderSender.CalculateOrderTimestamp(now.UtcDateTime, "US");

    //    Assert.Equal(3, orders.GetArrayLength());

    //    var first = orders[0];
    //    Assert.Equal("USD/CAD", first.GetProperty("symbol").GetString());
    //    Assert.Equal("SELL", first.GetProperty("side").GetString());
    //    Assert.Equal(OrderSender.BuildOrderCode(64, expectedOrderTimestampUtc), first.GetProperty("code").GetString());
    //    Assert.Equal(0.1, first.GetProperty("size").GetProperty("value").GetDouble(), 6);

    //    var second = orders[1];
    //    Assert.Equal("EUR/USD", second.GetProperty("symbol").GetString());
    //    Assert.Equal("BUY", second.GetProperty("side").GetString());
    //    Assert.Equal(OrderSender.BuildOrderCode(66, expectedOrderTimestampUtc), second.GetProperty("code").GetString());
    //    Assert.Equal(0.2, second.GetProperty("size").GetProperty("value").GetDouble(), 6);

    //    var third = orders[2];
    //    Assert.Equal("USD/CHF", third.GetProperty("symbol").GetString());
    //    Assert.Equal("BUY", third.GetProperty("side").GetString());
    //    Assert.Equal(OrderSender.BuildOrderCode(136, expectedOrderTimestampUtc), third.GetProperty("code").GetString());
    //    Assert.Equal(0.2, third.GetProperty("size").GetProperty("value").GetDouble(), 6);
    //}

    //[Fact]
    //public async Task SendOrdersAsync_FlipsSideForReversedAllowedSymbol()
    //{
    //    HttpRequestMessage? captured = null;
    //    var handler = new Mock<HttpMessageHandler>();
    //    handler.Protected().Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
    //        .Callback<HttpRequestMessage, CancellationToken>((message, _) => captured = message)
    //        .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
    //        {
    //            Content = new StringContent("{\"ts\":\"\",\"orders\":[]}")
    //        });

    //    var client = new HttpClient(handler.Object) { BaseAddress = new Uri("http://wakett") };
    //    var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient("WakettApi") == client);
    //    var apiClient = new WakettApiClient(factory);

    //    var now = new DateTimeOffset(2024, 1, 2, 16, 30, 0, TimeSpan.Zero);
    //    var barTimeUtc = now.AddMinutes(-60).UtcDateTime;
    //    var weights = new List<OrderSender.NettedWeightRow>
    //    {
    //        new() { SecurityId = 200, ModelId = 1, BarTimeUtc = barTimeUtc, ModelRunId = 11, Weight = 0.2m }
    //    };

    //    var configuration = BuildConfiguration(new Dictionary<string, string?>());

    //    var symbolMap = new Dictionary<int, string>
    //    {
    //        [200] = "CHFUSD"
    //    };

    //    var context = new Mock<DapperContext>(configuration);
    //    context.Setup(c => c.CreateConnection()).Returns(Mock.Of<IDbConnection>());

    //    var logger = Mock.Of<ILogger<OrderSender>>();
    //    var timeProvider = new TestTimeProvider(now);

    //    var sender = new TestOrderSender(
    //        apiClient,
    //        context.Object,
    //        logger,
    //        configuration,
    //        timeProvider,
    //        weights,
    //        symbolMap,
    //        new OrderSender.ModelScheduleRow { Offset = 60, BarSize = 0 });

    //    await sender.SendOrdersAsync();

    //    handler.Protected().Verify(
    //        "SendAsync",
    //        Times.Once(),
    //        ItExpr.IsAny<HttpRequestMessage>(),
    //        ItExpr.IsAny<CancellationToken>());

    //    Assert.NotNull(captured);
    //    var payload = await captured!.Content.ReadAsStringAsync();
    //    using var document = JsonDocument.Parse(payload);
    //    var orders = document.RootElement.GetProperty("orders");

    //    var expectedOrderTimestampUtc = OrderSender.CalculateOrderTimestamp(now.UtcDateTime, "US");

    //    Assert.Equal(1, orders.GetArrayLength());

    //    var first = orders[0];
    //    Assert.Equal("USD/CHF", first.GetProperty("symbol").GetString());
    //    Assert.Equal("SELL", first.GetProperty("side").GetString());
    //    Assert.Equal(OrderSender.BuildOrderCode(200, expectedOrderTimestampUtc), first.GetProperty("code").GetString());
    //    Assert.Equal(0.2, first.GetProperty("size").GetProperty("value").GetDouble(), 6);
    //}

    //[Fact]
    //public async Task SendOrdersAsync_DoesNotSendWhenWeightsAreStale()
    //{
    //    var handler = new Mock<HttpMessageHandler>();
    //    handler.Protected().Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
    //        .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK));

    //    var client = new HttpClient(handler.Object) { BaseAddress = new Uri("http://wakett") };
    //    var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient("WakettApi") == client);
    //    var apiClient = new WakettApiClient(factory);

    //    var configuration = BuildConfiguration(new Dictionary<string, string?>());

    //    var symbolMap = new Dictionary<int, string>
    //    {
    //        [58] = "EURUSD"
    //    };

    //    var context = new Mock<DapperContext>(configuration);
    //    context.Setup(c => c.CreateConnection()).Returns(Mock.Of<IDbConnection>());

    //    var logger = Mock.Of<ILogger<OrderSender>>();

    //    var zone = TimeZoneInfo.FindSystemTimeZoneById(RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Central European Standard Time" : "Europe/Berlin");
    //    var nowLocal = new DateTime(2024, 1, 2, 12, 0, 0);
    //    var nowUtc = TimeZoneInfo.ConvertTimeToUtc(nowLocal, zone);
    //    var staleLocal = nowLocal.AddHours(-3);
    //    var staleUtc = TimeZoneInfo.ConvertTimeToUtc(staleLocal, zone);

    //    var timeProvider = new TestTimeProvider(new DateTimeOffset(nowUtc, TimeSpan.Zero));

    //    var weights = new List<OrderSender.NettedWeightRow>
    //    {
    //        new() { SecurityId = 58, ModelId = 1, BarTimeUtc = staleUtc, ModelRunId = 1, Weight = 0.2m }
    //    };

    //    var sender = new TestOrderSender(
    //        apiClient,
    //        context.Object,
    //        logger,
    //        configuration,
    //        timeProvider,
    //        weights,
    //        symbolMap,
    //        new OrderSender.ModelScheduleRow { Offset = 60, BarSize = 0 });

    //    await sender.SendOrdersAsync();

    //    handler.Protected().Verify(
    //        "SendAsync",
    //        Times.Never(),
    //        ItExpr.IsAny<HttpRequestMessage>(),
    //        ItExpr.IsAny<CancellationToken>());
    //}

    //[Fact]
    //public async Task SendOrdersAsync_UsesPreviousDayWeightsForFirstTrade()
    //{
    //    HttpRequestMessage? captured = null;
    //    var handler = new Mock<HttpMessageHandler>();
    //    handler.Protected().Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
    //        .Callback<HttpRequestMessage, CancellationToken>((message, _) => captured = message)
    //        .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
    //        {
    //            Content = new StringContent("{\"ts\":\"\",\"orders\":[]}")
    //        });

    //    var client = new HttpClient(handler.Object) { BaseAddress = new Uri("http://wakett") };
    //    var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient("WakettApi") == client);
    //    var apiClient = new WakettApiClient(factory);

    //    var configuration = BuildConfiguration(new Dictionary<string, string?>());

    //    var symbolMap = new Dictionary<int, string>
    //    {
    //        [58] = "EURUSD"
    //    };

    //    var context = new Mock<DapperContext>(configuration);
    //    context.Setup(c => c.CreateConnection()).Returns(Mock.Of<IDbConnection>());

    //    var logger = Mock.Of<ILogger<OrderSender>>();

    //    var zoneId = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Central European Standard Time" : "Europe/Berlin";
    //    var zone = TimeZoneInfo.FindSystemTimeZoneById(zoneId);

    //    var nowLocal = new DateTime(2024, 1, 2, 7, 0, 0);
    //    var nowUtc = TimeZoneInfo.ConvertTimeToUtc(nowLocal, zone);
    //    var previousDayLocal = new DateTime(2024, 1, 1, 21, 0, 0);
    //    var previousDayUtc = TimeZoneInfo.ConvertTimeToUtc(previousDayLocal, zone);

    //    var timeProvider = new TestTimeProvider(new DateTimeOffset(nowUtc, TimeSpan.Zero));

    //    var weights = new List<OrderSender.NettedWeightRow>
    //    {
    //        new() { SecurityId = 58, ModelId = 1, BarTimeUtc = previousDayUtc, ModelRunId = 1, Weight = 0.3m }
    //    };

    //    var sender = new TestOrderSender(
    //        apiClient,
    //        context.Object,
    //        logger,
    //        configuration,
    //        timeProvider,
    //        weights,
    //        symbolMap,
    //        new OrderSender.ModelScheduleRow { Offset = 60, BarSize = 0 });

    //    await sender.SendOrdersAsync();

    //    handler.Protected().Verify(
    //        "SendAsync",
    //        Times.Once(),
    //        ItExpr.IsAny<HttpRequestMessage>(),
    //        ItExpr.IsAny<CancellationToken>());

    //    Assert.NotNull(captured);
    //    var payload = await captured!.Content.ReadAsStringAsync();
    //    using var document = JsonDocument.Parse(payload);
    //    var root = document.RootElement;
    //    var expectedTsUtc = OrderSender.CalculateOrderTimestamp(nowUtc, "US");
    //    Assert.Equal(OrderSender.FormatTimestamp(expectedTsUtc), root.GetProperty("ts").GetString());
    //}

    //[Fact]
    //public void CalculateOrderTimestamp_UsesNextQuarterHourWithinSession()
    //{
    //    var zoneId = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Eastern Standard Time" : "America/New_York";
    //    var zone = TimeZoneInfo.FindSystemTimeZoneById(zoneId);
    //    var localNow = new DateTime(2024, 1, 2, 10, 2, 0, DateTimeKind.Unspecified);
    //    var utcNow = TimeZoneInfo.ConvertTimeToUtc(localNow, zone);

    //    var result = OrderSender.CalculateOrderTimestamp(utcNow, "US");

    //    var expectedLocal = new DateTime(2024, 1, 2, 10, 6, 0, DateTimeKind.Unspecified);
    //    var expectedUtc = TimeZoneInfo.ConvertTimeToUtc(expectedLocal, zone);

    //    Assert.Equal(expectedUtc, result);
    //}

    //[Fact]
    //public void CalculateOrderTimestamp_UsesHolidaySessionEndWhenConfigured()
    //{
    //    var zoneId = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Eastern Standard Time" : "America/New_York";
    //    var zone = TimeZoneInfo.FindSystemTimeZoneById(zoneId);
    //    var localNow = new DateTime(2024, 7, 4, 13, 5, 0, DateTimeKind.Unspecified);
    //    var utcNow = TimeZoneInfo.ConvertTimeToUtc(localNow, zone);

    //    var result = OrderSender.CalculateOrderTimestamp(utcNow, "US", true);

    //    var expectedLocal = new DateTime(2024, 7, 4, 13, 21, 0, DateTimeKind.Unspecified);
    //    var expectedUtc = TimeZoneInfo.ConvertTimeToUtc(expectedLocal, zone);

    //    Assert.Equal(expectedUtc, result);
    //}

    //[Fact]
    //public void CalculateOrderTimestamp_RollsForwardWhenPastSessionEnd()
    //{
    //    var zoneId = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Eastern Standard Time" : "America/New_York";
    //    var zone = TimeZoneInfo.FindSystemTimeZoneById(zoneId);
    //    var localNow = new DateTime(2024, 1, 2, 16, 10, 0, DateTimeKind.Unspecified);
    //    var utcNow = TimeZoneInfo.ConvertTimeToUtc(localNow, zone);

    //    var result = OrderSender.CalculateOrderTimestamp(utcNow, "US");

    //    var expectedLocal = new DateTime(2024, 1, 3, 9, 6, 0, DateTimeKind.Unspecified);
    //    var expectedUtc = TimeZoneInfo.ConvertTimeToUtc(expectedLocal, zone);

    //    Assert.Equal(expectedUtc, result);
    //}

    //[Fact]
    //public void CalculateOrderTimestamp_SkipsWeekendWhenAdvancing()
    //{
    //    var zoneId = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Eastern Standard Time" : "America/New_York";
    //    var zone = TimeZoneInfo.FindSystemTimeZoneById(zoneId);
    //    var localNow = new DateTime(2024, 1, 5, 16, 10, 0, DateTimeKind.Unspecified);
    //    var utcNow = TimeZoneInfo.ConvertTimeToUtc(localNow, zone);

    //    var result = OrderSender.CalculateOrderTimestamp(utcNow, "US");

    //    var expectedLocal = new DateTime(2024, 1, 8, 9, 6, 0, DateTimeKind.Unspecified);
    //    var expectedUtc = TimeZoneInfo.ConvertTimeToUtc(expectedLocal, zone);

    //    Assert.Equal(expectedUtc, result);
    //}

    //[Fact]
    //public void FormatTimestamp_UsesProvidedOrderTime()
    //{
    //    var zoneId = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Eastern Standard Time" : "America/New_York";
    //    var zone = TimeZoneInfo.FindSystemTimeZoneById(zoneId);
    //    var localOrderTime = new DateTime(2024, 2, 5, 9, 21, 0, DateTimeKind.Unspecified);
    //    var orderTimeUtc = TimeZoneInfo.ConvertTimeToUtc(localOrderTime, zone);

    //    var formatted = OrderSender.FormatTimestamp(orderTimeUtc);

    //    var offset = zone.GetUtcOffset(localOrderTime);
    //    var sign = offset < TimeSpan.Zero ? "-" : "+";
    //    var abs = offset.Duration();
    //    var expected = $"{localOrderTime:yyyy-MM-dd HH:mm:ss.fff}{sign}{abs.Hours:00}{abs.Minutes:00}";

    //    Assert.Equal(expected, formatted);
    //}

    //[Theory]
    //[InlineData("EU", "EU")]
    //[InlineData("US", "US")]
    //[InlineData("EUUS", "EUUS")]
    //public void ResolveTradingSession_RespectsAllowedConfiguration(string configuredSession, string expected)
    //{
    //    var configuration = BuildConfiguration(new Dictionary<string, string?>
    //    {
    //        ["ExternalApis:WakettApi:Session"] = configuredSession
    //    });

    //    var sender = CreateSessionResolvingSender(configuration);
    //    var result = sender.InvokeResolveTradingSession(new DateTime(2024, 1, 2, 12, 0, 0, DateTimeKind.Utc));

    //    Assert.Equal(expected, result);
    //}

    //[Fact]
    //public void ResolveTradingSession_PrefersAllowedSessionsWhenConfiguredSessionDisabled()
    //{
    //    var zoneId = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Eastern Standard Time" : "America/New_York";
    //    var zone = TimeZoneInfo.FindSystemTimeZoneById(zoneId);
    //    var localBar = new DateTime(2024, 1, 2, 10, 0, 0, DateTimeKind.Unspecified);
    //    var barTimeUtc = TimeZoneInfo.ConvertTimeToUtc(localBar, zone);

    //    var configuration = BuildConfiguration(new Dictionary<string, string?>
    //    {
    //        ["ExternalApis:WakettApi:Session"] = "AS"
    //    });

    //    var sender = CreateSessionResolvingSender(configuration);
    //    var result = sender.InvokeResolveTradingSession(barTimeUtc);

    //    Assert.Equal("EUUS", result);
    //}

    //private static IConfigurationRoot BuildConfiguration(IDictionary<string, string?> values)
    //{
    //    var defaults = new Dictionary<string, string?>
    //    {
    //        ["Programmes:0:ModelId"] = "1",
    //        ["Programmes:0:Session"] = "EUUS",
    //        ["Programmes:0:Timeframe"] = "60",
    //        ["ExternalApis:WakettApi:BasePairs:0"] = "AUDUSD",
    //        ["ExternalApis:WakettApi:BasePairs:1"] = "USDCAD",
    //        ["ExternalApis:WakettApi:BasePairs:2"] = "USDCHF",
    //        ["ExternalApis:WakettApi:BasePairs:3"] = "USDCNH",
    //        ["ExternalApis:WakettApi:BasePairs:4"] = "EURUSD",
    //        ["ExternalApis:WakettApi:BasePairs:5"] = "GBPUSD",
    //        ["ExternalApis:WakettApi:BasePairs:6"] = "USDJPY",
    //        ["ExternalApis:WakettApi:BasePairs:7"] = "USDMXN",
    //        ["ExternalApis:WakettApi:BasePairs:8"] = "USDNOK",
    //        ["ExternalApis:WakettApi:BasePairs:9"] = "NZDUSD",
    //        ["ExternalApis:WakettApi:BasePairs:10"] = "USDSEK",
    //        ["ExternalApis:WakettApi:BasePairs:11"] = "USDSGD",
    //        ["ExternalApis:WakettApi:BasePairs:12"] = "USDZAR"
    //    };

    //    foreach (var kvp in values)
    //    {
    //        defaults[kvp.Key] = kvp.Value;
    //    }

    //    return new ConfigurationBuilder().AddInMemoryCollection(defaults).Build();
    //}

    //private static TestOrderSender CreateSessionResolvingSender(IConfiguration configuration)
    //{
    //    var httpClient = new HttpClient();
    //    var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient(It.IsAny<string>()) == httpClient);
    //    var apiClient = new WakettApiClient(factory);
    //    var context = new Mock<DapperContext>(configuration);
    //    context.Setup(c => c.CreateConnection()).Returns(Mock.Of<IDbConnection>());
    //    var logger = Mock.Of<ILogger<OrderSender>>();

    //    return new TestOrderSender(
    //        apiClient,
    //        context.Object,
    //        logger,
    //        configuration,
    //        new TestTimeProvider(DateTimeOffset.UtcNow),
    //        Array.Empty<OrderSender.NettedWeightRow>(),
    //        new Dictionary<int, string>(),
    //        null);
    //}

    //private sealed class TestOrderSender : OrderSender
    //{
    //    private readonly IReadOnlyList<OrderSender.NettedWeightRow> _weights;
    //    private readonly IReadOnlyDictionary<int, string> _symbolMap;
    //    private readonly ModelScheduleRow? _schedule;
    //    private readonly TradingLimitRow? _tradingLimit;
    //    private readonly List<TradingLimitBreachResult> _loggedBreaches = new();
    //    private readonly IReadOnlyCollection<string> _existingOrderSymbols;

    //    public TestOrderSender(
    //        WakettApiClient client,
    //        DapperContext context,
    //        ILogger<OrderSender> logger,
    //        IConfiguration configuration,
    //        TimeProvider timeProvider,
    //        IReadOnlyList<OrderSender.NettedWeightRow> weights,
    //        IReadOnlyDictionary<int, string> symbolMap,
    //        ModelScheduleRow? schedule,
    //        TradingLimitRow? tradingLimit = null,
    //        IReadOnlyCollection<string>? existingOrderSymbols = null,
    //        IDatabaseObjectNameProvider? databaseNameProvider = null,
    //        TradingOptions? tradingOptions = null)
    //        : base(
    //            client,
    //            context,
    //            logger,
    //            configuration,
    //            databaseNameProvider ?? DefaultDatabaseNameProvider,
    //            timeProvider,
    //            null,
    //            tradingOptions is null ? null : Options.Create(tradingOptions))
    //    {
    //        _weights = weights;
    //        _symbolMap = symbolMap;
    //        _schedule = schedule;
    //        _tradingLimit = tradingLimit;
    //        _existingOrderSymbols = existingOrderSymbols ?? Array.Empty<string>();
    //    }

    //    public IReadOnlyList<TradingLimitBreachResult> LoggedBreaches => _loggedBreaches;

    //    public static TradingLimitRow CreateTradingLimit(
    //        decimal? singleTradeGross = null,
    //        decimal? portfolioGross = null,
    //        decimal? portfolioNet = null,
    //        decimal? singleTradeTurnover = null,
    //        decimal? totalTurnover = null)
    //        => new()
    //        {
    //            ModelId = 1,
    //            SingleTradeGrossLimit = singleTradeGross,
    //            PortfolioGrossLimit = portfolioGross,
    //            PortfolioNetLimit = portfolioNet,
    //            SingleTradeTurnoverLimit = singleTradeTurnover,
    //            TotalTurnoverLimit = totalTurnover
    //        };

    //    protected override Task<IReadOnlyList<OrderSender.NettedWeightRow>> LoadLatestNettedWeightsAsync(
    //        IDbConnection connection,
    //        CancellationToken cancellationToken)
    //        => Task.FromResult(_weights);

    //    protected override Task<Dictionary<int, string>> LoadSymbolMapAsync(
    //        IDbConnection connection,
    //        CancellationToken cancellationToken)
    //        => Task.FromResult(new Dictionary<int, string>(_symbolMap));

    //    protected override Task<ModelScheduleRow?> LoadModelScheduleAsync(
    //        IDbConnection connection,
    //        CancellationToken cancellationToken)
    //        => Task.FromResult(_schedule);

    //    protected override Task<TradingLimitRow?> LoadTradingLimitsAsync(
    //        IDbConnection connection,
    //        CancellationToken cancellationToken)
    //        => Task.FromResult(_tradingLimit);

    //    protected override Task LogTradingLimitBreachesAsync(
    //        IDbConnection connection,
    //        IReadOnlyList<TradingLimitBreachResult> breaches,
    //        IReadOnlyList<(int SecurityId, WakettOrderItem Order)> orders,
    //        double? aum,
    //        CancellationToken cancellationToken)
    //    {
    //        _loggedBreaches.AddRange(breaches);
    //        return Task.CompletedTask;
    //    }

    //    protected override Task<IReadOnlyCollection<string>> LoadExistingOrderSymbolsAsync(
    //        IDbConnection connection,
    //        DateTimeOffset scheduledTimestamp,
    //        IEnumerable<(int SecurityId, WakettOrderItem Order)> builtOrders,
    //        CancellationToken cancellationToken)
    //        => Task.FromResult(_existingOrderSymbols);

    //    public IReadOnlyList<OrderSender.NettedWeightRow> InvokeApplyWeightOverrides(
    //        IEnumerable<OrderSender.NettedWeightRow> weights,
    //        IReadOnlyDictionary<int, string> symbolMap,
    //        DateTime orderTimestampUtc)
    //        => ApplyWeightOverrides(weights, symbolMap, orderTimestampUtc).ToList();

    //    public string InvokeResolveTradingSession(DateTime barTimeUtc)
    //        => ResolveTradingSession(barTimeUtc);
    //}

    //private sealed class TestTimeProvider : TimeProvider
    //{
    //    private readonly DateTimeOffset _utcNow;

    //    public TestTimeProvider(DateTimeOffset utcNow)
    //    {
    //        _utcNow = utcNow;
    //    }

    //    public override DateTimeOffset GetUtcNow() => _utcNow;
    //}
}
