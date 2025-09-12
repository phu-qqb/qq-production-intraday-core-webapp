using Microsoft.AspNetCore.OpenApi;
using TradingDaemon.Services;

namespace TradingDaemon.Controllers;

public static class PriceController
{
    public static void MapPriceEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/prices/fetch", async (PriceFetcher priceFetcher) =>
        {
            await priceFetcher.FetchAndStoreAsync();
            return Results.Ok(new { Status = "PricesFetched" });
        })
        .WithName("FetchPrices")
        .WithOpenApi(op =>
        {
            op.Summary = "Runs the price fetcher";
            op.Description = "Fetches price data from the configured CSV file and stores it in the database.";
            return op;
        });
    }
}
