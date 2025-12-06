using Microsoft.AspNetCore.OpenApi;
using TradingDaemon.Models;
using TradingDaemon.Services;

namespace TradingDaemon.Controllers;

public static class SlippageController
{
    public static void MapSlippageEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/slippage/run", async (
            SlippageRequest request,
            SlippageAndMissedCostService service,
            CancellationToken cancellationToken) =>
        {
            var result = await service.ComputeAsync(request, cancellationToken);
            return Results.Ok(result);
        })
        .WithName("ComputeSlippage")
        .WithOpenApi(op =>
        {
            op.Summary = "Computes slippage and missed trade costs for a trading day.";
            op.Description = "Calculates the difference between theoretical and real PnL for the requested date using Wakett orders, fills, and price bars.";
            return op;
        });
    }
}
