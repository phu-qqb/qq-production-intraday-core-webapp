using Microsoft.AspNetCore.OpenApi;
using TradingDaemon.Models;
using TradingDaemon.Services;

namespace TradingDaemon.Controllers;

public static class WakettController
{
    public static void MapWakettEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/wakett/prices", async (WakettApiClient client, IConfiguration config, WakettPriceRequest request) =>
        {
            var symbols = config.GetSection("ExternalApis:WakettApi:Symbols").Get<List<WakettSecuritySymbol>>() ?? new();
            var prices = await client.GetPricesAsync(symbols, request.Ts);
            return Results.Ok(prices);
        })
        .WithName("FetchWakettPrices")
        .WithOpenApi(op =>
        {
            op.Summary = "Fetch prices from Wakett";
            op.Description = "Calls the Wakett API to retrieve prices for the configured symbols.";
            return op;
        });
    }
}
