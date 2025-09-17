using Microsoft.AspNetCore.OpenApi;
using TradingDaemon.Models;
using TradingDaemon.Services;

namespace TradingDaemon.Controllers;

public static class WakettController
{
    public static void MapWakettEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/wakett/prices", async (WakettPriceFetcher fetcher, WakettPriceRequest request) =>
        {
            var result = await fetcher.FetchAndStoreAsync(request.Ts);
            return result is not null
                ? Results.Ok(result)
                : Results.Ok(new { Uploaded = 0 });
        })
        .WithName("FetchWakettPrices")
        .WithOpenApi(op =>
        {
            op.Summary = "Fetch and store Wakett FX prices.";
            op.Description = "Retrieves FX prices from Wakett, reconstructs missing crosses, and stores them in the database.";
            return op;
        });
    }
}
