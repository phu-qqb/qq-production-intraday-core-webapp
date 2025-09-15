using Microsoft.AspNetCore.OpenApi;
using TradingDaemon.Models;
using TradingDaemon.Services;

namespace TradingDaemon.Controllers;

public static class WakettController
{
    public static void MapWakettEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/wakett/prices", async (WakettApiClient client, WakettPriceRequest request) =>
        {
            var prices = await client.GetPricesAsync(request.Symbols, request.Ts);
            return Results.Ok(prices);
        })
        .WithName("FetchWakettPrices")
        .WithOpenApi(op =>
        {
            op.Summary = "Fetch prices from Wakett";
            op.Description = "Calls the Wakett API to retrieve prices for the specified symbols.";
            return op;
        });
    }
}
