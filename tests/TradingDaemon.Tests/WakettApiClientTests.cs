using Xunit;
using Moq;
using Moq.Protected;
using System.Net;
using System.Net.Http;
using TradingDaemon.Services;
using TradingDaemon.Models;

public class WakettApiClientTests
{
    [Fact]
    public async Task GetPricesAsync_PostsToPricesEndpoint()
    {
        HttpRequestMessage? captured = null;
        var handler = new Mock<HttpMessageHandler>();
        handler.Protected().Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
            .Callback<HttpRequestMessage, CancellationToken>((m, _) => captured = m)
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("{\"ts\":\"\",\"prices\":[]}") });
        var client = new HttpClient(handler.Object) { BaseAddress = new Uri("http://test") };
        var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient("WakettApi") == client);
        var api = new WakettApiClient(factory);

        await api.GetPricesAsync(new[] { new WakettSecuritySymbol { SecurityId = 1, Symbol = "AAPL" } });

        handler.Protected().Verify(
            "SendAsync",
            Times.Once(),
            ItExpr.Is<HttpRequestMessage>(m => m.Method == HttpMethod.Post && m.RequestUri!.PathAndQuery == "/prices"),
            ItExpr.IsAny<CancellationToken>());

        var body = await captured!.Content.ReadAsStringAsync();
        Assert.Contains("\"symbols\":[\"AAPL\"]", body);
        Assert.DoesNotContain("\"securityid\"", body);
    }

    [Fact]
    public async Task SendOrdersAsync_PostsToOrdersEndpoint()
    {
        var handler = new Mock<HttpMessageHandler>();
        handler.Protected().Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("{\"ts\":\"\",\"orders\":[]}") });
        var client = new HttpClient(handler.Object) { BaseAddress = new Uri("http://test") };
        var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient("WakettApi") == client);
        var api = new WakettApiClient(factory);

        var request = new WakettOrderRequest
        {
            Aum = 1_000_000,
            Orders = new List<WakettOrderItem>
            {
                new() { Symbol = "AAPL", Side = "BUY", Code = "QQB-1", Size = new WakettOrderSize{ Value = 100, Type = "absolute"} }
            }
        };

        await api.SendOrdersAsync(request);

        handler.Protected().Verify(
            "SendAsync",
            Times.Once(),
            ItExpr.Is<HttpRequestMessage>(m => m.Method == HttpMethod.Post && m.RequestUri!.PathAndQuery == "/orders"),
            ItExpr.IsAny<CancellationToken>());
    }

    [Fact]
    public async Task GetTradesAsync_PostsToTradesEndpoint()
    {
        var handler = new Mock<HttpMessageHandler>();
        handler.Protected().Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("{\"status\":\"OK\",\"message\":\"\",\"data\":[]}") });
        var client = new HttpClient(handler.Object) { BaseAddress = new Uri("http://test") };
        var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient("WakettApi") == client);
        var api = new WakettApiClient(factory);

        var request = new WakettTradeRequest { Account = "ACC", From = "20240101", To = "20240101", Strategy = "QQB" };
        await api.GetTradesAsync(request);

        handler.Protected().Verify(
            "SendAsync",
            Times.Once(),
            ItExpr.Is<HttpRequestMessage>(m => m.Method == HttpMethod.Post && m.RequestUri!.PathAndQuery == "/trades"),
            ItExpr.IsAny<CancellationToken>());
    }
}
