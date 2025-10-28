using System.Threading;
using Microsoft.AspNetCore.OpenApi;
using TradingDaemon.Services;

namespace TradingDaemon.Controllers;

public static class PriceController
{
    public static void MapPriceEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/prices/fetch", async (
            WakettPriceFetcher priceFetcher,
            CancellationToken cancellationToken) =>
        {
            await priceFetcher.FetchAndStoreAsync(cancellationToken);
            return Results.Ok(new { Status = "PricesFetched" });
        })
        .WithName("FetchPrices")
        .WithOpenApi(op =>
        {
            op.Summary = "Fetches the latest Wakett FX prices.";
            op.Description = "Retrieves FX prices from Wakett and stores them in the intraday price tables.";
            return op;
        });
    }
}
