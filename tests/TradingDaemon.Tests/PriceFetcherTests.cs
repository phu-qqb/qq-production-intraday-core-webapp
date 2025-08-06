using Moq;
using Moq.Protected;
using System.Net;
using System.Net.Http;
using TradingDaemon.Services;
using TradingDaemon.Data;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;

public class PriceFetcherTests
{
    [Fact]
    public async Task FetchAndStoreAsync_CallsApi()
    {
        var handler = new Mock<HttpMessageHandler>();
        handler.Protected().Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("[]") });
        var client = new HttpClient(handler.Object) { BaseAddress = new Uri("http://test") };
        var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient("PriceApi") == client);
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { ["ConnectionStrings:DefaultConnection"] = "" }).Build();
        var context = new DapperContext(config);
        var logger = Mock.Of<ILogger<PriceFetcher>>();
        var fetcher = new PriceFetcher(factory, context, logger);

        await fetcher.FetchAndStoreAsync();

        handler.Protected().Verify("SendAsync", Times.Once(), ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>());
    }
}
