using Polly;
using Polly.Extensions.Http;

namespace TradingDaemon.Utils;

public static class RetryPolicyFactory
{
    public static IAsyncPolicy<HttpResponseMessage> GetPolicy()
        => HttpPolicyExtensions
            .HandleTransientHttpError()
            .WaitAndRetryAsync(3, attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)));
}
