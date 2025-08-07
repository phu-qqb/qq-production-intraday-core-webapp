using TradingDaemon.Services;

namespace TradingDaemon.Controllers;

public static class TradingController
{
    public static void MapTradingEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/trading/run", async (PriceFetcher priceFetcher, WeightCalculator weightCalculator, OrderSender orderSender) =>
        {
            await priceFetcher.FetchAndStoreAsync();
            await weightCalculator.CalculateAndStoreAsync();
            await orderSender.SendOrdersAsync();
            return Results.Ok(new { Status = "Completed" });
        });
    }
}
