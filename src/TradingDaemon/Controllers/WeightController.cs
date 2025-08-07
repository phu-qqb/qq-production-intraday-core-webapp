using Microsoft.AspNetCore.OpenApi;
using TradingDaemon.Services;

namespace TradingDaemon.Controllers;

public static class WeightController
{
    public static void MapWeightEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/weights/calculate", async (WeightCalculator weightCalculator) =>
        {
            await weightCalculator.CalculateAndStoreAsync();
            return Results.Ok(new { Status = "WeightsCalculated" });
        })
        .WithName("CalculateWeights")
        .WithOpenApi(op =>
        {
            op.Summary = "Runs the GPU weight calculation";
            op.Description = "Executes the external GPU process to compute asset weights and store the results.";
            return op;
        });
    }
}
