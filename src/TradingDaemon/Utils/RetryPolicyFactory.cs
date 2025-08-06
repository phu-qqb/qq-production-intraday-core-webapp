using Polly;
using System.Net;

namespace TradingDaemon.Utils;

public static class RetryPolicyFactory
{
    public static IAsyncPolicy<HttpResponseMessage> GetPolicy()
        => Policy<HttpResponseMessage>
            .Handle<HttpRequestException>()
            .OrResult(response =>
                response.StatusCode == HttpStatusCode.RequestTimeout ||
                (int)response.StatusCode >= 500)
            .WaitAndRetryAsync(3, attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)));
}
