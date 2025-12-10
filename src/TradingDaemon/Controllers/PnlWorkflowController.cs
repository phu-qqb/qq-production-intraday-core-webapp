using System.Collections.Generic;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.OpenApi;
using Microsoft.OpenApi.Any;
using Microsoft.OpenApi.Models;
using TradingDaemon.Models;
using TradingDaemon.Services;

namespace TradingDaemon.Controllers;

public static class PnlWorkflowController
{
    public static void MapPnlWorkflowEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/pnl/workflow/run", async Task<Results<Ok<PnlWorkflowResponse>, ProblemHttpResult>> (
            PnlWorkflowRunner workflowRunner,
            ILogger<PnlWorkflowEndpointsLogger> logger,
            CancellationToken cancellationToken) =>
        {
            PnlWorkflowResult? result;

            try
            {
                result = await workflowRunner.RunAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }

            if (result is null)
            {
                return TypedResults.Problem(
                    title: "PnL workflow failed.",
                    detail: "See server logs for additional details.",
                    statusCode: StatusCodes.Status500InternalServerError);
            }

            logger.LogInformation(
                "PnL automation workflow completed for {TradingDate} with PnL {Pnl}.",
                result.Report.TradingDate,
                result.Report.Pnl);

            return TypedResults.Ok(new PnlWorkflowResponse(
                result.Report.TradingDate,
                result.Report.Pnl,
                result.Report.GrossMarketValue,
                result.Report.TotalNetExposure,
                result.SlippageResult));
        })
        .WithName("RunAutomatedPnlWorkflow")
        .Produces<PnlWorkflowResponse>()
        .ProducesProblem(StatusCodes.Status500InternalServerError)
        .WithOpenApi(op =>
        {
            op.Summary = "Run the Wakett PnL email automation.";
            op.Description =
                "Computes slippage and missed trade costs for the current trading day, recomputes and stores the PnL report, " +
                "and sends the automated PnL email notification.";

            op.Responses[StatusCodes.Status200OK.ToString()] = new OpenApiResponse
            {
                Description = "The PnL workflow finished and the report email was sent.",
                Content = new Dictionary<string, OpenApiMediaType>
                {
                    ["application/json"] = new OpenApiMediaType
                    {
                        Schema = new OpenApiSchema
                        {
                            Reference = new OpenApiReference
                            {
                                Type = ReferenceType.Schema,
                                Id = nameof(PnlWorkflowResponse)
                            }
                        },
                        Example = new OpenApiObject
                        {
                            ["tradingDate"] = new OpenApiString("2024-01-15"),
                            ["pnl"] = new OpenApiDouble(125000.42),
                            ["grossMarketValue"] = new OpenApiDouble(3500000.00),
                            ["totalNetExposure"] = new OpenApiDouble(-150000.25),
                            ["slippageResult"] = new OpenApiObject
                            {
                                ["tradingDate"] = new OpenApiString("2024-01-15"),
                                ["theoreticalPnl"] = new OpenApiDouble(130000.00),
                                ["realizedPnl"] = new OpenApiDouble(125000.42),
                                ["missedTradeCost"] = new OpenApiDouble(2500.00),
                                ["slippage"] = new OpenApiDouble(4499.58)
                            }
                        }
                    }
                }
            };

            op.Responses[StatusCodes.Status500InternalServerError.ToString()] = new OpenApiResponse
            {
                Description = "The PnL workflow failed to complete."
            };

            return op;
        });
    }
}

public sealed record PnlWorkflowResponse(
    DateOnly TradingDate,
    decimal Pnl,
    decimal GrossMarketValue,
    decimal TotalNetExposure,
    SlippageResult? SlippageResult);

internal sealed class PnlWorkflowEndpointsLogger;
