using Microsoft.AspNetCore.OpenApi;
using TradingDaemon.Models;
using TradingDaemon.Services;

namespace TradingDaemon.Controllers;

public static class ReportController
{
    public static void MapReportEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/reports/run", async (ReportRequest request, ReportRunner runner) =>
        {
            var (_, err, exitCode) = await runner.RunAsync(request);
            if (exitCode != 0)
            {
                return Results.Problem(err, statusCode: 500);
            }
            return Results.Ok(new { Status = "ReportGenerated" });
        })
        .WithName("RunReport")
        .WithOpenApi(op =>
        {
            op.Summary = "Runs the reporting script";
            op.Description = "Launches the Python reporting script to generate model performance reports.";
            return op;
        });
    }
}
